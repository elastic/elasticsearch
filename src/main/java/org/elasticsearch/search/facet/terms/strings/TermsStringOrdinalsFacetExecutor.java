/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
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

    final CacheRecycler cacheRecycler;
    final BigArrays bigArrays;
    private final TermsFacet.ComparatorType comparatorType;
    private final int size;
    private final int shardSize;
    private final int minCount;
    private final ImmutableSet<BytesRef> excluded;
    private final Matcher matcher;
    final int ordinalsCacheAbove;

    final List<ReaderAggregator> aggregators;
    long missing;
    long total;

    public TermsStringOrdinalsFacetExecutor(IndexFieldData.WithOrdinals indexFieldData, int size, int shardSize, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                            ImmutableSet<BytesRef> excluded, Pattern pattern, int ordinalsCacheAbove) {
        this.indexFieldData = indexFieldData;
        this.size = size;
        this.shardSize = shardSize;
        this.comparatorType = comparatorType;
        this.ordinalsCacheAbove = ordinalsCacheAbove;

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

        this.cacheRecycler = context.cacheRecycler();
        this.bigArrays = context.bigArrays();

        this.aggregators = new ArrayList<>(context.searcher().getIndexReader().leaves().size());
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        final CharsRef spare = new CharsRef();
        AggregatorPriorityQueue queue = new AggregatorPriorityQueue(aggregators.size());
        for (ReaderAggregator aggregator : aggregators) {
            if (aggregator.nextPosition()) {
                queue.add(aggregator);
            }
        }

        // YACK, we repeat the same logic, but once with an optimizer priority queue for smaller sizes
        if (shardSize < EntryPriorityQueue.LIMIT) {
            // optimize to use priority size
            EntryPriorityQueue ordered = new EntryPriorityQueue(shardSize, comparatorType.comparator());

            while (queue.size() > 0) {
                ReaderAggregator agg = queue.top();
                BytesRef value = agg.copyCurrent(); // we need to makeSafe it, since we end up pushing it... (can we get around this?)
                int count = 0;
                do {
                    count += agg.counts.get(agg.position);
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
                    if (matcher != null) {
                        UnicodeUtil.UTF8toUTF16(value, spare);
                        assert spare.toString().equals(value.utf8ToString());
                        if (!matcher.reset(spare).matches()) {
                            continue;
                        }
                    }
                    InternalStringTermsFacet.TermEntry entry = new InternalStringTermsFacet.TermEntry(value, count);
                    ordered.insertWithOverflow(entry);
                }
            }
            InternalStringTermsFacet.TermEntry[] list = new InternalStringTermsFacet.TermEntry[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (InternalStringTermsFacet.TermEntry) ordered.pop();
            }

            Releasables.close(aggregators);

            return new InternalStringTermsFacet(facetName, comparatorType, size, Arrays.asList(list), missing, total);
        }

        BoundedTreeSet<InternalStringTermsFacet.TermEntry> ordered = new BoundedTreeSet<>(comparatorType.comparator(), shardSize);

        while (queue.size() > 0) {
            ReaderAggregator agg = queue.top();
            BytesRef value = agg.copyCurrent(); // we need to makeSafe it, since we end up pushing it... (can we work around that?)
            int count = 0;
            do {
                count += agg.counts.get(agg.position);
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
                if (matcher != null) {
                    UnicodeUtil.UTF8toUTF16(value, spare);
                    assert spare.toString().equals(value.utf8ToString());
                    if (!matcher.reset(spare).matches()) {
                        continue;
                    }
                }
                InternalStringTermsFacet.TermEntry entry = new InternalStringTermsFacet.TermEntry(value, count);
                ordered.add(entry);
            }
        }

        Releasables.close(aggregators);

        return new InternalStringTermsFacet(facetName, comparatorType, size, ordered, missing, total);
    }

    class Collector extends FacetExecutor.Collector {

        private long missing;
        private long total;
        private BytesValues.WithOrdinals values;
        private ReaderAggregator current;
        private Ordinals.Docs ordinals;

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            if (current != null) {
                missing += current.missing;
                total += current.total;
                if (current.values.ordinals().getMaxOrd() > Ordinals.MIN_ORDINAL) {
                    aggregators.add(current);
                } else {
                    Releasables.close(current);
                }
            }
            values = indexFieldData.load(context).getBytesValues(false);
            current = new ReaderAggregator(values, ordinalsCacheAbove, cacheRecycler);
            ordinals = values.ordinals();
        }

        @Override
        public void collect(int doc) throws IOException {
            final int length = ordinals.setDocument(doc);
            int missing = 1;
            for (int i = 0; i < length; i++) {
                current.onOrdinal(doc, ordinals.nextOrd());
                missing = 0;
            }
            current.incrementMissing(missing);
        }

        @Override
        public void postCollection() {
            if (current != null) {
                missing += current.missing;
                total += current.total;
                // if we have values for this one, add it
                if (current.values.ordinals().getMaxOrd() > Ordinals.MIN_ORDINAL) {
                    aggregators.add(current);
                } else {
                    Releasables.close(current);
                }
                current = null;
            }
            TermsStringOrdinalsFacetExecutor.this.missing = missing;
            TermsStringOrdinalsFacetExecutor.this.total = total;
        }
    }

    public final class ReaderAggregator implements Releasable {

        private final long maxOrd;

        final BytesValues.WithOrdinals values;
        final IntArray counts;
        int missing = 0;
        long position = Ordinals.MIN_ORDINAL - 1;
        BytesRef current;
        int total;


        public ReaderAggregator(BytesValues.WithOrdinals values, int ordinalsCacheLimit, CacheRecycler cacheRecycler) {
            this.values = values;
            this.maxOrd = values.ordinals().getMaxOrd();
            this.counts = bigArrays.newIntArray(maxOrd);
        }

        final void onOrdinal(int docId, long ordinal) {
            counts.increment(ordinal, 1);
            total++;
        }

        final void incrementMissing(int numMissing) {
            missing += numMissing;
        }

        public boolean nextPosition() {
            if (++position >= maxOrd) {
                return false;
            }
            current = values.getValueByOrd(position);
            return true;
        }

        public BytesRef copyCurrent() {
            return values.copyShared();
        }

        @Override
        public void close() {
            Releasables.close(counts);
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
