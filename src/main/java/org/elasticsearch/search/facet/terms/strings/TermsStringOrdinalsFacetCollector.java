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
import org.elasticsearch.index.fielddata.IndexOrdinalFieldData;
import org.elasticsearch.index.fielddata.OrdinalsBytesValues;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
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
public class TermsStringOrdinalsFacetCollector extends AbstractFacetCollector {

    private final IndexOrdinalFieldData indexFieldData;

    private final TermsFacet.ComparatorType comparatorType;

    private final int size;

    private final int numberOfShards;

    private final int minCount;

    private OrdinalsBytesValues values;

    private final List<ReaderAggregator> aggregators;

    private ReaderAggregator current;

    long missing;
    long total;

    private final ImmutableSet<BytesRef> excluded;

    private final Matcher matcher;

    public TermsStringOrdinalsFacetCollector(String facetName, IndexOrdinalFieldData indexFieldData, int size, TermsFacet.ComparatorType comparatorType, boolean allTerms, SearchContext context,
                                             ImmutableSet<BytesRef> excluded, Pattern pattern) {
        super(facetName);
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
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        if (current != null) {
            missing += current.counts[0];
            total += current.total - current.counts[0];
            if (current.values.ordinals().getNumOrds() > 1) {
                aggregators.add(current);
            }
        }
        values = indexFieldData.load(context).getBytesValues();
        current = new ReaderAggregator(values);
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        values.ordinals().forEachOrdinalInDoc(doc, current);
    }

    @Override
    public Facet facet() {
        if (current != null) {
            missing += current.counts[0];
            total += current.total - current.counts[0];
            // if we have values for this one, add it
            if (current.values.ordinals().getNumOrds() > 1) {
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

    public static class ReaderAggregator implements Ordinals.Docs.OrdinalInDocProc {

        final OrdinalsBytesValues values;
        final int[] counts;

        int position = 0;
        BytesRef current;
        int total;

        public ReaderAggregator(OrdinalsBytesValues values) {
            this.values = values;
            this.counts = CacheRecycler.popIntArray(values.ordinals().getNumOrds());
        }

        @Override
        public void onOrdinal(int docId, int ordinal) {
            counts[ordinal]++;
            total++;
        }

        public boolean nextPosition() {
            if (++position >= values.ordinals().getNumOrds()) {
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
