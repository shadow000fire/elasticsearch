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
import org.apache.lucene.search.grouping.*;
import org.apache.lucene.search.grouping.AbstractSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class GroupingSearchGroupsPhase {

        private final String groupField;
        private final ValueSource groupFunction;
        private final Map<?, ?> valueSourceContext;

        private Sort groupSort = Sort.RELEVANCE;
        private Sort withinGroupSort = Sort.RELEVANCE;

        private boolean includeScores = true;
        private boolean includeMaxScore = true;

        private Collection<?> matchingGroups;

        /**
         * Constructs a <code>GroupingSearch</code> instance that groups documents by index terms using the {@link FieldCache}.
         * The group field can only have one token per document. This means that the field must not be analysed.
         *
         * @param groupField The name of the field to group by.
         */
        public GroupingSearchGroupsPhase(String groupField) {
          this(groupField, null, null);
        }

        /**
         * Constructs a <code>GroupingSearch</code> instance that groups documents by function using a {@link ValueSource}
         * instance.
         *
         * @param groupFunction      The function to group by specified as {@link ValueSource}
         * @param valueSourceContext The context of the specified groupFunction
         */
        public GroupingSearchGroupsPhase(ValueSource groupFunction, Map<?, ?> valueSourceContext) {
          this(null, groupFunction, valueSourceContext);
        }


        private GroupingSearchGroupsPhase(String groupField, ValueSource groupFunction, Map<?, ?> valueSourceContext) {
          this.groupField = groupField;
          this.groupFunction = groupFunction;
          this.valueSourceContext = valueSourceContext;
        }

        /**
         * Executes a grouped search. Both the first pass and second pass are executed on the specified searcher.
         *
         * @param searcher    The {@link org.apache.lucene.search.IndexSearcher} instance to execute the grouped search on.
         * @param query       The query to execute with the grouping
         * @param groups      Number of top groups to identify
         * @return the grouped result as a {@link TopGroups} instance
         * @throws IOException If any I/O related errors occur
         */
        public <T> TopGroups<T> search(IndexSearcher searcher, Query query, int groups) throws IOException {
          return search(searcher, null, query, groups);
        }

        /**
         * Executes a grouped search. Both the first pass and second pass are executed on the specified searcher.
         *
         * @param searcher    The {@link org.apache.lucene.search.IndexSearcher} instance to execute the grouped search on.
         * @param filter      The filter to execute with the grouping
         * @param query       The query to execute with the grouping
         * @param groupOffset The group offset
         * @param groupLimit  The number of groups to return from the specified group offset
         * @return the grouped result as a {@link TopGroups} instance
         * @throws IOException If any I/O related errors occur
         */
        @SuppressWarnings("unchecked")
        public <T> TopGroups<T> search(IndexSearcher searcher, Filter filter, Query query, int groups) throws IOException {
          if (groupField != null || groupFunction != null) {
            return groupByFieldOrFunction(searcher, filter, query, groups);
          } else {
            throw new IllegalStateException("Either groupField, groupFunction or groupEndDocs must be set."); // This can't happen...
          }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        protected TopGroups groupByFieldOrFunction(IndexSearcher searcher, Filter filter, Query query, int groups) throws IOException {
          
          /*
           * TODO This could be done in a single pass with a Collector that saved the top hit for each group
           * and its sort fields.  But for now this will do functionally.  
           */
          final AbstractFirstPassGroupingCollector firstPassCollector;
          if (groupFunction != null) {
            firstPassCollector = new FunctionFirstPassGroupingCollector(groupFunction, valueSourceContext, groupSort, groups);
          } else {
            firstPassCollector = new TermFirstPassGroupingCollector(groupField, groupSort, groups);
          }
          searcher.search(query, filter, firstPassCollector);

          Collection<SearchGroup> topSearchGroups = firstPassCollector.getTopGroups(0, false);
          if (topSearchGroups == null) {
            return new TopGroups(new SortField[0], new SortField[0], 0, 0, new GroupDocs[0], Float.NaN);
          }
          
          AbstractSecondPassGroupingCollector secondPassCollector;
          if (groupFunction != null) {
            secondPassCollector = new FunctionSecondPassGroupingCollector((Collection) topSearchGroups, groupSort, withinGroupSort, 1, includeScores, includeMaxScore, true, groupFunction, valueSourceContext);
          } else {
            secondPassCollector = new TermSecondPassGroupingCollector(groupField, (Collection) topSearchGroups, groupSort, withinGroupSort, 1, includeScores, includeMaxScore, true);
          }

          searcher.search(query, filter, secondPassCollector);
          return secondPassCollector.getTopGroups(0);
        }


        /**
         * Specifies how groups are sorted.
         * Defaults to {@link Sort#RELEVANCE}.
         *
         * @param groupSort The sort for the groups.
         * @return <code>this</code>
         */
        public GroupingSearchGroupsPhase setGroupSort(Sort groupSort) {
          this.groupSort = groupSort;
          return this;
        }

        /**
         * Specifies how hits within groups are sorted.
         * Defaults to {@link Sort#RELEVANCE}.
         *
         * @param groupSort The sort for the groups.
         * @return <code>this</code>
         */
        public GroupingSearchGroupsPhase setWithinGroupSort(Sort withinGroupSort) {
          this.withinGroupSort = withinGroupSort;
          return this;
        }

        /**
         * Whether to include the score of the most relevant document per group.
         *
         * @param includeMaxScore Whether to include the score of the most relevant document per group
         * @return <code>this</code>
         */
        public GroupingSearchGroupsPhase setIncludeMaxScore(boolean includeMaxScore) {
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

