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

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.TopGroups;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.InternalFacets;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

import static org.elasticsearch.common.lucene.Lucene.*;

/**
 *
 */
public class QuerySearchResult extends TransportResponse implements QuerySearchResultProvider {

    private long id;
    private SearchShardTarget shardTarget;
    private int from;
    private int size;
    private int groupSize;
    private int groupFrom;
    private TopDocs topDocs;
    private TopGroups topGroups;
    private boolean groupsPhase=true;//false is hits phase
    private InternalFacets facets;
    private Suggest suggest;
    private boolean searchTimedOut;

    public QuerySearchResult() {

    }

    public QuerySearchResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    @Override
    public boolean includeFetch() {
        return false;
    }

    @Override
    public QuerySearchResult queryResult() {
        return this;
    }

    public long id() {
        return this.id;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
    }

    public void searchTimedOut(boolean searchTimedOut) {
        this.searchTimedOut = searchTimedOut;
    }

    public boolean searchTimedOut() {
        return searchTimedOut;
    }

    public TopDocs topDocs() {
        return topDocs;
    }

    public void topDocs(TopDocs topDocs) {
        this.topDocs = topDocs;
    }
    
    public TopGroups topGroups() {
        return topGroups;
    }

    public void topGroups(TopGroups topGroups) {
        this.topGroups = topGroups;
    }
    
    public boolean groupsPhase()
    {
        return groupsPhase;
    }
    
    public void groupsPhase(boolean groupsPhase)
    {
        this.groupsPhase=groupsPhase;
    }

    public Facets facets() {
        return facets;
    }

    public void facets(InternalFacets facets) {
        this.facets = facets;
    }

    public Suggest suggest() {
        return suggest;
    }

    public void suggest(Suggest suggest) {
        this.suggest = suggest;
    }

    public int from() {
        return from;
    }

    public QuerySearchResult from(int from) {
        this.from = from;
        return this;
    }

    public int size() {
        return size;
    }

    public QuerySearchResult size(int size) {
        this.size = size;
        return this;
    }
    
    public int groupSize() {
        return groupSize;
    }

    public QuerySearchResult groupSize(int groupSize) {
        this.groupSize = groupSize;
        return this;
    }
    
    public int groupFrom() {
        return groupFrom;
    }

    public QuerySearchResult groupFrom(int groupFrom) {
        this.groupFrom = groupFrom;
        return this;
    }

    public static QuerySearchResult readQuerySearchResult(StreamInput in) throws IOException {
        QuerySearchResult result = new QuerySearchResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
//        shardTarget = readSearchShardTarget(in);
        from = in.readVInt();
        size = in.readVInt();
        groupSize=in.readVInt();
        groupFrom=in.readVInt();
        topDocs = readTopDocs(in);
        topGroups=readTopGroups(in);
        groupsPhase=in.readBoolean();
        if (in.readBoolean()) {
            facets = InternalFacets.readFacets(in);
        }
        if (in.readBoolean()) {
            suggest = Suggest.readSuggest(Suggest.Fields.SUGGEST, in);
        }
        searchTimedOut = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
//        shardTarget.writeTo(out);
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeVInt(groupSize);
        out.writeVInt(groupFrom);
        writeTopDocs(out, topDocs, 0);
        writeTopGroups(out,topGroups,0,0);
        out.writeBoolean(groupsPhase);
        if (facets == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            facets.writeTo(out);
        }
        if (suggest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            suggest.writeTo(out);
        }
        out.writeBoolean(searchTimedOut);
    }
}
