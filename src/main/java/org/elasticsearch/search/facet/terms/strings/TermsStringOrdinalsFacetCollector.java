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

package org.elasticsearch.search.facet.terms.strings;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.strings.StringFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class TermsStringOrdinalsFacetCollector extends AbstractFacetCollector {

    private final FieldDataCache fieldDataCache;

    private final String indexFieldName;

    private String groupFieldName;
    private FieldDataType groupFieldType;
    private final List<GroupedFacetHitEntry> previousHits = new LinkedList<GroupedFacetHitEntry>();

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final int minCount;

    private final FieldDataType fieldDataType;

    private StringFieldData fieldData;

    private final List<ReaderAggregator> aggregators;

    private ReaderAggregator current;

    long missing;
    long total;

    private final ImmutableSet<String> excluded;

    private final Matcher matcher;

    SearchContext context;

    public TermsStringOrdinalsFacetCollector(String facetName, String fieldName, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                             ImmutableSet<String> excluded, Pattern pattern) {
        super(facetName);
        this.fieldDataCache = context.fieldDataCache();
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();
        this.context = context;

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] doesn't have a type, can't run terms long facet collector on it");
        }
        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.explicitTypeInNameWithDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        if (smartMappers.mapper().fieldDataType() != FieldDataType.DefaultTypes.STRING) {
            throw new ElasticSearchIllegalArgumentException("Field [" + fieldName + "] is not of string type, can't run terms string facet collector on it");
        }

        this.indexFieldName = smartMappers.mapper().names().indexName();
        this.fieldDataType = smartMappers.mapper().fieldDataType();

        if (excluded == null || excluded.isEmpty()) {
            this.excluded = null;
        } else {
            this.excluded = excluded;
        }
        this.matcher = pattern != null ? pattern.matcher("") : null;

        // minCount is offset by -1
        if (allTerms) {
            minCount = -1;
        } else {
            minCount = 0;
        }

        this.aggregators = new ArrayList<ReaderAggregator>(context.searcher().subReaders().length);
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        if (current != null) {
            missing += current.counts[0];
            total += current.total - current.counts[0];
            if (current.values.length > 1) {
                aggregators.add(current);
            }
        }
        fieldData = (StringFieldData) fieldDataCache.cache(fieldDataType, reader, indexFieldName);
        if (grouped()) {
            StringFieldData groupData = (StringFieldData) fieldDataCache.cache(groupFieldType, reader, groupFieldName);
            current = new GroupedReaderAggregator(fieldData, groupData, previousHits);
        } else {
            current = new NonGroupedReaderAggregator(fieldData);
        }
    }

    @Override 
    public void grouped(String groupFieldName) {
        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(groupFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
          throw new ElasticSearchIllegalArgumentException("Field [" + groupFieldName + "] doesn't have a type, can't run terms long facet collector on it");
        }
        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper() && smartMappers.explicitTypeInName()) {
          setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }
    
        if (smartMappers.mapper().fieldDataType() != FieldDataType.DefaultTypes.STRING) {
          throw new ElasticSearchIllegalArgumentException("Field [" + groupFieldName + "] is not of string type, can't run terms string facet collector on it");
        }
    
        this.groupFieldName = smartMappers.mapper().names().indexName();
        this.groupFieldType = smartMappers.mapper().fieldDataType();
    }

    @Override 
    public boolean grouped() {
      return groupFieldName != null;
    }
  
    @Override
    protected void doCollect(int doc) throws IOException {
        fieldData.forEachOrdinalInDoc(doc, current);
    }

    @Override
    public Facet facet() {
        if (current != null) {
            missing += current.counts[0];
            total += current.total - current.counts[0];
            // if we have values for this one, add it
            if (current.values.length > 1) {
                aggregators.add(current);
            }
        }

        AggregatorPriorityQueue queue = new AggregatorPriorityQueue(aggregators.size());

        for (ReaderAggregator aggregator : aggregators) {
            if (aggregator.nextPosition()) {
                queue.add(aggregator);
            }
        }

        // YACK, we repeat the same logic, but once with an optimizer priority queue for smaller sizes
        if (size < EntryPriorityQueue.LIMIT) {
            // optimize to use priority size
            EntryPriorityQueue ordered = new EntryPriorityQueue(size, comparatorType.comparator());

            while (queue.size() > 0) {
                ReaderAggregator agg = queue.top();
                String value = agg.current;
                int count = 0;
                do {
                    count += agg.counts[agg.position];
                    if (agg.nextPosition()) {
                        agg = queue.updateTop();
                    } else {
                        // we are done with this reader
                        queue.pop();
                        agg = queue.top();
                    }
                } while (agg != null && value.equals(agg.current));

                if (count > minCount) {
                    if (excluded != null && excluded.contains(value)) {
                        continue;
                    }
                    if (matcher != null && !matcher.reset(value).matches()) {
                        continue;
                    }
                    InternalStringTermsFacet.StringEntry entry = new InternalStringTermsFacet.StringEntry(value, count);
                    ordered.insertWithOverflow(entry);
                }
            }
            InternalStringTermsFacet.StringEntry[] list = new InternalStringTermsFacet.StringEntry[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (InternalStringTermsFacet.StringEntry) ordered.pop();
            }

            for (ReaderAggregator aggregator : aggregators) {
                CacheRecycler.pushIntArray(aggregator.counts);
            }

            return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
        }

        BoundedTreeSet<InternalStringTermsFacet.StringEntry> ordered = new BoundedTreeSet<InternalStringTermsFacet.StringEntry>(comparatorType.comparator(), size);

        while (queue.size() > 0) {
            ReaderAggregator agg = queue.top();
            String value = agg.current;
            int count = 0;
            do {
                count += agg.counts[agg.position];
                if (agg.nextPosition()) {
                    agg = queue.updateTop();
                } else {
                    // we are done with this reader
                    queue.pop();
                    agg = queue.top();
                }
            } while (agg != null && value.equals(agg.current));

            if (count > minCount) {
                if (excluded != null && excluded.contains(value)) {
                    continue;
                }
                if (matcher != null && !matcher.reset(value).matches()) {
                    continue;
                }
                InternalStringTermsFacet.StringEntry entry = new InternalStringTermsFacet.StringEntry(value, count);
                ordered.add(entry);
            }
        }


        for (ReaderAggregator aggregator : aggregators) {
            CacheRecycler.pushIntArray(aggregator.counts);
        }

        return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing, total);
    }

    abstract static class ReaderAggregator implements FieldData.OrdinalInDocProc {

        final String[] values;
        final int[] counts;

        int position = 0;
        String current;
        int total;

        public ReaderAggregator(StringFieldData fieldData) {
            this.values = fieldData.values();
            this.counts = CacheRecycler.popIntArray(fieldData.values().length);
        }

        public boolean nextPosition() {
            if (++position >= values.length) {
                return false;
            }
            current = values[position];
            return true;
        }
    }

    static class NonGroupedReaderAggregator extends ReaderAggregator {

        public NonGroupedReaderAggregator(StringFieldData fieldData) {
            super(fieldData);
        }

        @Override public void onOrdinal(int docId, int ordinal) {
            counts[ordinal]++;
            total++;
        }
    }

    static class GroupedReaderAggregator extends ReaderAggregator {

        final FixedBitSet checker;
        final StringFieldData groupData;
        final List<GroupedFacetHitEntry> previousHits;

        public GroupedReaderAggregator(StringFieldData fieldData, StringFieldData groupData, List<GroupedFacetHitEntry> previousHits) {
            super(fieldData);
            this.groupData = groupData;
            this.previousHits = previousHits;
            this.checker = new FixedBitSet(groupData.values().length * values.length);
            for (GroupedFacetHitEntry previousHit : previousHits) {
                    int facetOrdinal = fieldData.binarySearchLookup(previousHit.facetValue);
                    if (facetOrdinal < 0) {
                        continue;
                    }

                    int groupOrdinal = groupData.binarySearchLookup(previousHit.groupValue);
                    if (groupOrdinal < 0) {
                        continue;
                    }

                    int checkerIndex = (groupOrdinal * values.length) + facetOrdinal;
                    checker.set(checkerIndex);
            }
        }

        @Override public void onOrdinal(int docId, int ordinal) {
            int groupOrdinal = groupData.ordinal(docId);
            int checkerIndex = (groupOrdinal * values.length) + ordinal;

            if (!checker.get(checkerIndex)) {
                counts[ordinal]++;
                total++;
                checker.set(checkerIndex);
                previousHits.add(new GroupedFacetHitEntry(values[ordinal], groupData.values()[groupOrdinal]));

            }
        }
    }

    // To keep track of the cross segment counts
    static class GroupedFacetHitEntry {

        final String facetValue;
        final String groupValue;

        GroupedFacetHitEntry(String facetValue, String groupValue) {
            this.facetValue = facetValue;
            this.groupValue = groupValue;
        }
    }

    public static class AggregatorPriorityQueue extends PriorityQueue<ReaderAggregator> {

        public AggregatorPriorityQueue(int size) {
            initialize(size);
        }

        @Override
        protected boolean lessThan(ReaderAggregator a, ReaderAggregator b) {
            return a.current.compareTo(b.current) < 0;
        }
    }
}
