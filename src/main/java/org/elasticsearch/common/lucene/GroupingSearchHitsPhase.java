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

package org.elasticsearch.common.lucene;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class GroupingSearchHitsPhase {

        private final String groupField;
        private final ValueSource groupFunction;
        private final Map<?, ?> valueSourceContext;

        private Sort groupSort = Sort.RELEVANCE;
        private Sort withinGroupSort = Sort.RELEVANCE;

        private boolean includeScores = true;
        private boolean includeMaxScore = true;

        private Collection<?> matchingGroups;
        private Collection<SearchGroup<MutableValue>> topSearchGroups;

        /**
         * Constructs a <code>GroupingSearch</code> instance that groups documents by index terms using the {@link FieldCache}.
         * The group field can only have one token per document. This means that the field must not be analysed.
         *
         * @param groupField The name of the field to group by.
         */
        public GroupingSearchHitsPhase(String groupField, Collection<SearchGroup<MutableValue>> topSearchGroups) {
          this(groupField, null, null,topSearchGroups);
        }

        /**
         * Constructs a <code>GroupingSearch</code> instance that groups documents by function using a {@link ValueSource}
         * instance.
         *
         * @param groupFunction      The function to group by specified as {@link ValueSource}
         * @param valueSourceContext The context of the specified groupFunction
         */
        public GroupingSearchHitsPhase(ValueSource groupFunction, Map<?, ?> valueSourceContext,Collection<SearchGroup<MutableValue>> topSearchGroups) {
          this(null, groupFunction, valueSourceContext,topSearchGroups);
        }


        private GroupingSearchHitsPhase(String groupField, ValueSource groupFunction, Map<?, ?> valueSourceContext,Collection<SearchGroup<MutableValue>> topSearchGroups) {
          this.groupField = groupField;
          this.groupFunction = groupFunction;
          this.valueSourceContext = valueSourceContext;
          this.topSearchGroups=topSearchGroups;
        }

        /**
         * Executes a grouped search. Both the first pass and second pass are executed on the specified searcher.
         *
         * @param searcher    The {@link org.apache.lucene.search.IndexSearcher} instance to execute the grouped search on.
         * @param query       The query to execute with the grouping
         * @param groupLimit  The number of groups to return from the specified group offset
         * @return the grouped result as a {@link TopGroups} instance
         * @throws IOException If any I/O related errors occur
         */
        public <T> TopGroups<T> search(IndexSearcher searcher, Query query, int groupLimit) throws IOException {
          return search(searcher, null, query, groupLimit);
        }

        /**
         * Executes a grouped search. Both the first pass and second pass are executed on the specified searcher.
         *
         * @param searcher    The {@link org.apache.lucene.search.IndexSearcher} instance to execute the grouped search on.
         * @param filter      The filter to execute with the grouping
         * @param query       The query to execute with the grouping
         * @param groupLimit  The number of groups to return from the specified group offset
         * @return the grouped result as a {@link TopGroups} instance
         * @throws IOException If any I/O related errors occur
         */
        @SuppressWarnings("unchecked")
        public <T> TopGroups<T> search(IndexSearcher searcher, Filter filter, Query query, int groupLimit) throws IOException {
          if (groupField != null || groupFunction != null) {
            return groupByFieldOrFunction(searcher, filter, query, groupLimit);
          } else {
            throw new IllegalStateException("Either groupField, groupFunction or groupEndDocs must be set."); // This can't happen...
          }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        protected TopGroups groupByFieldOrFunction(IndexSearcher searcher, Filter filter, Query query, int groupLimit) throws IOException {
          AbstractSecondPassGroupingCollector secondPassCollector;
          if (groupFunction != null) {
            secondPassCollector = new FunctionSecondPassGroupingCollector((Collection) topSearchGroups, groupSort, withinGroupSort, groupLimit, includeScores, includeMaxScore, true, groupFunction, valueSourceContext);
          } else {
            secondPassCollector = new TermSecondPassGroupingCollector(groupField, (Collection) topSearchGroups, groupSort, withinGroupSort, groupLimit, includeScores, includeMaxScore, true);
          }

          searcher.search(query, filter, secondPassCollector);
          return secondPassCollector.getTopGroups(0, false);
        }


        /**
         * Specifies how groups are sorted.
         * Defaults to {@link Sort#RELEVANCE}.
         *
         * @param groupSort The sort for the groups.
         * @return <code>this</code>
         */
        public GroupingSearchHitsPhase setGroupSort(Sort groupSort) {
          this.groupSort = groupSort;
          return this;
        }
        
        /**
         * Specifies how groups are sorted.
         * Defaults to {@link Sort#RELEVANCE}.
         *
         * @param groupSort The sort for the groups.
         * @return <code>this</code>
         */
        public GroupingSearchHitsPhase setWithinGroupSort(Sort withinGroupSort) {
          this.withinGroupSort = withinGroupSort;
          return this;
        }



        /**
         * Whether to include the score of the most relevant document per group.
         *
         * @param includeMaxScore Whether to include the score of the most relevant document per group
         * @return <code>this</code>
         */
        public GroupingSearchHitsPhase setIncludeMaxScore(boolean includeMaxScore) {
          this.includeMaxScore = includeMaxScore;
          return this;
        }

        /**
         * If {@link #setAllGroups(boolean)} was set to <code>true</code> then all matching groups are returned, otherwise
         * an empty collection is returned.
         *
         * @param <T> The group value type. This can be a {@link BytesRef} or a {@link MutableValue} instance. If grouping
         *            by doc block this the group value is always <code>null</code>.
         * @return all matching groups are returned, or an empty collection
         */
        @SuppressWarnings({"unchecked", "rawtypes"})
        public <T> Collection<T> getAllMatchingGroups() {
          return (Collection<T>) matchingGroups;
        }
      }

