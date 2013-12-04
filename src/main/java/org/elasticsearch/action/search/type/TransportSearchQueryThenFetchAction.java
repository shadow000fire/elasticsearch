/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search.type;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ReduceSearchPhaseException;
import org.elasticsearch.action.search.SearchOperationThreading;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.ExtTIntArrayList;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TransportSearchQueryThenFetchAction extends TransportSearchTypeAction {

    @Inject
    public TransportSearchQueryThenFetchAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                               SearchServiceTransportAction searchService, SearchPhaseController searchPhaseController) {
        super(settings, threadPool, clusterService, searchService, searchPhaseController);
    }

    @Override
    protected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        new AsyncAction(searchRequest, listener).start();
    }

    private class AsyncAction extends BaseAsyncAction<QuerySearchResult> {

        final AtomicArray<FetchSearchResult> fetchResults;
        final AtomicArray<ExtTIntArrayList> docIdsToLoad;

        private AsyncAction(SearchRequest request, ActionListener<SearchResponse> listener) {
            super(request, listener);
            fetchResults = new AtomicArray<FetchSearchResult>(firstResults.length());
            docIdsToLoad = new AtomicArray<ExtTIntArrayList>(firstResults.length());
        }

        @Override
        protected String firstPhaseName() {
            return "query";
        }

        @Override
        protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchRequest request, SearchServiceListener<QuerySearchResult> listener) {
            searchService.sendExecuteQuery(node, request, listener);
        }

        @Override
        protected void moveToSecondPhase() {
            List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> results=firstResults.asList();
            
            if(results.get(0).value.queryResult().topGroups()!=null) // means we got back groups from the query
            {
                try
                {
                    if(results.get(0).value.queryResult().groupsPhase())
                    {
                        logger.info("Coordinator - Merging Groups Phase");
                        //merge top groups, reset request object, call start
                        GroupDocs[] groupDocs = searchPhaseController.sortGroupedDocs(results, results.get(0).value.queryResult().size(), 1, 
                                results.get(0).value.queryResult().from(),false);
                        if(groupDocs.length==0)
                        {
                            sortedShardList=SearchPhaseController.EMPTY_DOCS;
                            prepareFetch();
                        }
                        else
                        {
                            SearchGroup[] topGroups=new SearchGroup[groupDocs.length];
                            for(int ii=0;ii<groupDocs.length;ii++)
                            {
                                topGroups[ii]=new SearchGroup();
                                topGroups[ii].groupValue=groupDocs[ii].groupValue;
                                topGroups[ii].sortValues=groupDocs[ii].groupSortValues;
                                logger.info("Top Group: value={}, score={}, hits={}",  topGroups[ii].groupValue, groupDocs[ii].maxScore,groupDocs[ii].totalHits);
                            }
                            
                            new GroupAsyncAction(request,listener, topGroups).start();
                        }
                    }
                }catch(IOException e)
                {
                    logger.error("Exception encountered while processing group query", e);
                    results.clear();
                }
            }
            else
            {
                sortedShardList = searchPhaseController.sortDocs(results);
                prepareFetch();
            }
        }
        
        protected void prepareFetch()
        {
            searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);

            if (docIdsToLoad.asList().isEmpty()) {
                finishHim();
                return;
            }

            final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());

            int localOperations = 0;
            for (AtomicArray.Entry<ExtTIntArrayList> entry : docIdsToLoad.asList()) {
                QuerySearchResult queryResult = firstResults.get(entry.index);
                DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
                if (node.id().equals(nodes.localNodeId())) {
                    localOperations++;
                } else {
                    FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(request, queryResult.id(), entry.value);
                    executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, node);
                }
            }

            if (localOperations > 0) {
                if (request.operationThreading() == SearchOperationThreading.SINGLE_THREAD) {
                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                        @Override
                        public void run() {
                            for (AtomicArray.Entry<ExtTIntArrayList> entry : docIdsToLoad.asList()) {
                                QuerySearchResult queryResult = firstResults.get(entry.index);
                                DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
                                if (node.id().equals(nodes.localNodeId())) {
                                    FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(request, queryResult.id(), entry.value);
                                    executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, node);
                                }
                            }
                        }
                    });
                } else {
                    boolean localAsync = request.operationThreading() == SearchOperationThreading.THREAD_PER_SHARD;
                    for (final AtomicArray.Entry<ExtTIntArrayList> entry : docIdsToLoad.asList()) {
                        final QuerySearchResult queryResult = firstResults.get(entry.index);
                        final DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
                        if (node.id().equals(nodes.localNodeId())) {
                            final FetchSearchRequest fetchSearchRequest = new FetchSearchRequest(request, queryResult.id(), entry.value);
                            try {
                                if (localAsync) {
                                    threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                                        @Override
                                        public void run() {
                                            executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, node);
                                        }
                                    });
                                } else {
                                    executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, node);
                                }
                            } catch (Throwable t) {
                                onFetchFailure(t, fetchSearchRequest, entry.index, queryResult.shardTarget(), counter);
                            }
                        }
                    }
                }
            }
        }

        void executeFetch(final int shardIndex, final SearchShardTarget shardTarget, final AtomicInteger counter, final FetchSearchRequest fetchSearchRequest, DiscoveryNode node) {
            searchService.sendExecuteFetch(node, fetchSearchRequest, new SearchServiceListener<FetchSearchResult>() {
                @Override
                public void onResult(FetchSearchResult result) {
                    result.shardTarget(shardTarget);
                    fetchResults.set(shardIndex, result);
                    if (counter.decrementAndGet() == 0) {
                        finishHim();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    onFetchFailure(t, fetchSearchRequest, shardIndex, shardTarget, counter);
                }
            });
        }

        void onFetchFailure(Throwable t, FetchSearchRequest fetchSearchRequest, int shardIndex, SearchShardTarget shardTarget, AtomicInteger counter) {
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Failed to execute fetch phase", t, fetchSearchRequest.id());
            }
            this.addShardFailure(shardIndex, shardTarget, t);
            successulOps.decrementAndGet();
            if (counter.decrementAndGet() == 0) {
                finishHim();
            }
        }

        void finishHim() {
            try {
                innerFinishHim();
            } catch (Throwable e) {
                ReduceSearchPhaseException failure = new ReduceSearchPhaseException("fetch", "", e, buildShardFailures());
                if (logger.isDebugEnabled()) {
                    logger.debug("failed to reduce search", failure);
                }
                listener.onFailure(failure);
            } finally {
                releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
            }
        }

        void innerFinishHim() throws Exception {
            InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, firstResults, fetchResults);
            String scrollId = null;
            if (request.scroll() != null) {
                scrollId = TransportSearchHelper.buildScrollId(request.searchType(), firstResults, null);
            }
            listener.onResponse(new SearchResponse(internalResponse, scrollId, expectedSuccessfulOps, successulOps.get(), buildTookInMillis(), buildShardFailures()));
        }
    }
    
    private class GroupAsyncAction extends AsyncAction {

        private GroupAsyncAction(SearchRequest request, ActionListener<SearchResponse> listener, SearchGroup[] topGroups) {
            super(request, listener);
            this.topGroups=topGroups;
        }

        @Override
        protected void moveToSecondPhase() {
            List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> results=firstResults.asList();
            
            try
            {
                logger.info("Coordinator - Merging Hits Phase");
                
                //merge groups of hits and call fetch
                GroupDocs[]  groupDocs = searchPhaseController.sortGroupedDocs(results, results.get(0).value.queryResult().size(), 
                        results.get(0).value.queryResult().groupSize(),results.get(0).value.queryResult().from(),true);
                ArrayList<ScoreDoc> docList=new ArrayList<ScoreDoc>(groupDocs.length*results.get(0).value.queryResult().groupSize());
                for(GroupDocs group:groupDocs)
                {
                    for(ScoreDoc doc:group.scoreDocs)
                    {
                        docList.add(doc);
                    }
                }
                sortedShardList=docList.toArray(new ScoreDoc[docList.size()]);
                prepareFetch();
            }catch(IOException e)
            {
                logger.error("Exception encountered while processing group query", e);
                results.clear();
            }
        }
    }
}
