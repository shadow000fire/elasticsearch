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

package org.elasticsearch.search.query;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.GroupingSearchGroupsPhase;
import org.elasticsearch.common.lucene.GroupingSearchHitsPhase;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.facet.FacetPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.sort.GroupSortParseElement;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.search.sort.TrackScoresParseElement;
import org.elasticsearch.search.suggest.SuggestPhase;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class QueryPhase implements SearchPhase {

    private final FacetPhase facetPhase;
    private final SuggestPhase suggestPhase;
    private RescorePhase rescorePhase;

    @Inject
    public QueryPhase(FacetPhase facetPhase, SuggestPhase suggestPhase, RescorePhase rescorePhase) {
        this.facetPhase = facetPhase;
        this.suggestPhase = suggestPhase;
        this.rescorePhase = rescorePhase;
    }

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        ImmutableMap.Builder<String, SearchParseElement> parseElements = ImmutableMap.builder();
        parseElements.put("from", new FromParseElement()).put("size", new SizeParseElement())
                .put("groupSize", new GroupSizeParseElement())
                .put("group_size", new GroupSizeParseElement())
                .put("group_from", new GroupFromParseElement())
                .put("groupFrom", new GroupFromParseElement())
                .put("groupBy",new GroupByParseElement())
                .put("group_by",new GroupByParseElement())
                .put("indices_boost", new IndicesBoostParseElement())
                .put("indicesBoost", new IndicesBoostParseElement())
                .put("query", new QueryParseElement())
                .put("queryBinary", new QueryBinaryParseElement())
                .put("query_binary", new QueryBinaryParseElement())
                .put("filter", new FilterParseElement())
                .put("filterBinary", new FilterBinaryParseElement())
                .put("filter_binary", new FilterBinaryParseElement())
                .put("sort", new SortParseElement())
                .put("groupSort", new GroupSortParseElement())
                .put("group_sort", new GroupSortParseElement())
                .put("trackScores", new TrackScoresParseElement())
                .put("track_scores", new TrackScoresParseElement())
                .put("min_score", new MinScoreParseElement())
                .put("minScore", new MinScoreParseElement())
                .put("timeout", new TimeoutParseElement())
                .putAll(facetPhase.parseElements())
                .putAll(suggestPhase.parseElements())
                .putAll(rescorePhase.parseElements());
        return parseElements.build();
    }

    @Override
    public void preProcess(SearchContext context) {
        context.preProcess();
        facetPhase.preProcess(context);
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        searchContext.queryResult().searchTimedOut(false);

        List<SearchContext.Rewrite> rewrites = searchContext.rewrites();
        if (rewrites != null) {
            try {
                searchContext.searcher().inStage(ContextIndexSearcher.Stage.REWRITE);
                for (SearchContext.Rewrite rewrite : rewrites) {
                    rewrite.contextRewrite(searchContext);
                }
            } catch (Exception e) {
                throw new QueryPhaseExecutionException(searchContext, "failed to execute context rewrite", e);
            } finally {
                searchContext.searcher().finishStage(ContextIndexSearcher.Stage.REWRITE);
            }
        }

        searchContext.searcher().inStage(ContextIndexSearcher.Stage.MAIN_QUERY);
        boolean rescore = false;
        try {
            searchContext.queryResult().from(searchContext.from());
            searchContext.queryResult().size(searchContext.size());

            Query query = searchContext.query();

            TopDocs topDocs=null;
            int numDocs, numGroups;
            
            if(searchContext.groupBy()==null)
            {
                numDocs = searchContext.from() + searchContext.size();
                if (numDocs == 0) {
                    // if 0 was asked, change it to 1 since 0 is not allowed
                    numDocs = 1;
                }
                numGroups=-1;//not used
            }
            else
            {
                if(searchContext.request().topGroups()==null)//Groups Phase
                {
                    numDocs=-1;//not used
                    numGroups=searchContext.from() + searchContext.size();
                }
                else //Hits Phase
                {
                    numDocs=searchContext.groupFrom()+searchContext.groupSize();
                    numGroups=searchContext.size();
                }
                searchContext.queryResult().groupSize(searchContext.groupSize());
                searchContext.queryResult().groupFrom(searchContext.groupFrom());
            }

            if (searchContext.searchType() == SearchType.COUNT) {
                TotalHitCountCollector collector = new TotalHitCountCollector();
                searchContext.searcher().search(query, collector);
                topDocs = new TopDocs(collector.getTotalHits(), Lucene.EMPTY_SCORE_DOCS, 0);
            } else if (searchContext.searchType() == SearchType.SCAN) {
                topDocs = searchContext.scanContext().execute(searchContext);
            } 
            else if(searchContext.groupBy()!=null)
            {
                //defaults to sort groups and hits in group by RELEVANCE
//                topN groups where N=maxGroups*groupSize;
                
                //This is the old approach that doesn'tr gaurantee best results after hit #1 for each group
//                GroupingSearch search=new GroupingSearch(searchContext.groupBy()).setAllGroupHeads(false).setAllGroups(true)
//                        .setFillSortFields(true).setGroupDocsLimit(searchContext.groupSize());
//                searchContext.queryResult().topGroups(
//                        search.search(searchContext.searcher(), query, searchContext.from(), searchContext.groupSize()));
                ESLogger logger=Loggers.getLogger(getClass());
                int shardId=searchContext.indexShard().shardId().getId();
                
                if(searchContext.request().topGroups()==null)
                {
                    logger.info("{} - Group query - Groups Phase", shardId);
                    //first phase, find the top groups (1 hit each just for sorting)
                    GroupingSearchGroupsPhase search=new GroupingSearchGroupsPhase(searchContext.groupBy());
                    if(searchContext.sort()!=null)
                    {
                        search.setGroupSort(searchContext.sort());
                    }
                    if(searchContext.groupSort()!=null)
                    {
                        search.setWithinGroupSort(searchContext.groupSort());
                    }
                    searchContext.queryResult().groupsPhase(true);
                    searchContext.queryResult().topGroups(
                          search.search(searchContext.searcher(), query, numGroups));
                    for (GroupDocs group: searchContext.queryResult().topGroups().groups)
                    {
                        logger.info("{} - Found group: {} with top score {}, maxScore={}, totalHits={}",shardId,((BytesRef)group.groupValue).utf8ToString(),group.scoreDocs[0].score, group.maxScore, group.totalHits);
                    }
                }
                else
                {
                    logger.info("{} - Group query - Hits Phase", shardId);
                    //second phase, find top N hits only for specified groups
                    GroupingSearchHitsPhase search=new GroupingSearchHitsPhase(searchContext.groupBy(),
                            (Collection)Arrays.asList(searchContext.request().topGroups()));
                    if(searchContext.sort()!=null)
                    {
                        search.setGroupSort(searchContext.sort());
                    }
                    if(searchContext.groupSort()!=null)
                    {
                        search.setWithinGroupSort(searchContext.groupSort());
                    }
                    searchContext.queryResult().groupsPhase(false);
                    searchContext.queryResult().topGroups(
                          search.search(searchContext.searcher(), query, numDocs));
                    for (GroupDocs group: searchContext.queryResult().topGroups().groups)
                    {
                        logger.info("{} - Found group: {} with top score {}, maxScore={}, totalHits={}, scoreDocs.length={}",shardId,((BytesRef)group.groupValue).utf8ToString(),group.scoreDocs[0].score, group.maxScore, group.totalHits,group.scoreDocs.length);
                    }
                }
            }
            else if (searchContext.sort() != null) {
                topDocs = searchContext.searcher().search(query, null, numDocs, searchContext.sort(),
                        searchContext.trackScores(), searchContext.trackScores());
            } else {
                if (searchContext.rescore() != null) {
                    rescore = true;
                    numDocs = Math.max(searchContext.rescore().window(), numDocs);
                }
                topDocs = searchContext.searcher().search(query, numDocs);
            }
            searchContext.queryResult().topDocs(topDocs);
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        } finally {
            searchContext.searcher().finishStage(ContextIndexSearcher.Stage.MAIN_QUERY);
        }
        if (rescore) { // only if we do a regular search
            rescorePhase.execute(searchContext);
        }
        suggestPhase.execute(searchContext);
        facetPhase.execute(searchContext);

        if (rewrites != null) {
            for (SearchContext.Rewrite rewrite : rewrites) {
                rewrite.executionDone();
            }
        }
    }
}
