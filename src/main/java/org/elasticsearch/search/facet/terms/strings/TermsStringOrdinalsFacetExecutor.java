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
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.support.EntryPriorityQueue;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class TermsStringOrdinalsFacetExecutor extends FacetExecutor {

    private final IndexFieldData.WithOrdinals indexFieldData;

    private final TermsFacet.ComparatorType comparatorType;
    private final int size;
    private final int numberOfShards;
    private final int minCount;
    private final ImmutableSet<BytesRef> excluded;
    private final Matcher matcher;

    final List<ReaderAggregator> aggregators;
    long missing;
    long total;

    public TermsStringOrdinalsFacetExecutor(IndexFieldData.WithOrdinals indexFieldData, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                            ImmutableSet<BytesRef> excluded, Pattern pattern) {
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.comparatorType = comparatorType;
        this.numberOfShards = context.numberOfShards();

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

        this.aggregators = new ArrayList<ReaderAggregator>(context.searcher().getIndexReader().leaves().size());
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
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
                BytesRef value = agg.values.makeSafe(agg.current); // we need to makeSafe it, since we end up pushing it... (can we get around this?)
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
                    // LUCENE 4 UPGRADE: use Lucene's RegexCapabilities
                    if (matcher != null && !matcher.reset(value.utf8ToString()).matches()) {
                        continue;
                    }
                    InternalStringTermsFacet.TermEntry entry = new InternalStringTermsFacet.TermEntry(value, count);
                    ordered.insertWithOverflow(entry);
                }
            }
            InternalStringTermsFacet.TermEntry[] list = new InternalStringTermsFacet.TermEntry[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (InternalStringTermsFacet.TermEntry) ordered.pop();
            }

            for (ReaderAggregator aggregator : aggregators) {
                CacheRecycler.pushIntArray(aggregator.counts);
            }

            return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
        }

        BoundedTreeSet<InternalStringTermsFacet.TermEntry> ordered = new BoundedTreeSet<InternalStringTermsFacet.TermEntry>(comparatorType.comparator(), size);

        while (queue.size() > 0) {
            ReaderAggregator agg = queue.top();
            BytesRef value = agg.values.makeSafe(agg.current); // we need to makeSafe it, since we end up pushing it... (can we work around that?)
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
                // LUCENE 4 UPGRADE: use Lucene's RegexCapabilities
                if (matcher != null && !matcher.reset(value.utf8ToString()).matches()) {
                    continue;
                }
                InternalStringTermsFacet.TermEntry entry = new InternalStringTermsFacet.TermEntry(value, count);
                ordered.add(entry);
            }
        }


        for (ReaderAggregator aggregator : aggregators) {
            CacheRecycler.pushIntArray(aggregator.counts);
        }

        return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing, total);
    }

    class Collector extends FacetExecutor.Collector {

        private long missing;
        private long total;
        private BytesValues.WithOrdinals values;
        private ReaderAggregator current;

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            if (current != null) {
                missing += current.counts[0];
                total += current.total - current.counts[0];
                if (current.values.ordinals().getNumOrds() > 0) {
                    aggregators.add(current);
                }
            }
            values = indexFieldData.load(context).getBytesValues();
            current = new ReaderAggregator(values);
        }

        @Override
        public void collect(int doc) throws IOException {
            values.ordinals().forEachOrdinalInDoc(doc, current);
        }

        @Override
        public void postCollection() {
            if (current != null) {
                missing += current.counts[0];
                total += current.total - current.counts[0];
                // if we have values for this one, add it
                if (current.values.ordinals().getNumOrds() > 0) {
                    aggregators.add(current);
                }
                current = null;
            }
            TermsStringOrdinalsFacetExecutor.this.missing = missing;
            TermsStringOrdinalsFacetExecutor.this.total = total;
        }
    }

    public static class ReaderAggregator implements Ordinals.Docs.OrdinalInDocProc {

        final BytesValues.WithOrdinals values;
        final int[] counts;

        int position = 0;
        BytesRef current;
        int total;
        private final int maxOrd;

        public ReaderAggregator(BytesValues.WithOrdinals values) {
            this.values = values;
            this.maxOrd = values.ordinals().getMaxOrd();

            this.counts = CacheRecycler.popIntArray(maxOrd);
        }

        @Override
        public void onOrdinal(int docId, int ordinal) {
            counts[ordinal]++;
            total++;
        }

        public boolean nextPosition() {
            if (++position >= maxOrd) {
                return false;
            }
            current = values.getValueByOrd(position);
            return true;
        }
    }

    public static class AggregatorPriorityQueue extends PriorityQueue<ReaderAggregator> {

        public AggregatorPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(ReaderAggregator a, ReaderAggregator b) {
            return a.current.compareTo(b.current) < 0;
        }
    }
}
