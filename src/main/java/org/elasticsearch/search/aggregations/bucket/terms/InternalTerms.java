/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import com.google.common.collect.Maps;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.*;

/**
 *
 */
public abstract class InternalTerms extends InternalAggregation implements Terms, ToXContent, Streamable {

    public static abstract class Bucket implements Terms.Bucket {

        protected long docCount;
        protected InternalAggregations aggregations;

        protected Bucket(long docCount, InternalAggregations aggregations) {
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public int compareTo(Terms.Bucket o) {
            long i = compareTerm(o);
            if (i == 0) {
                i = docCount - o.getDocCount();
                if (i == 0) {
                    i = System.identityHashCode(this) - System.identityHashCode(o);
                }
            }
            return i > 0 ? 1 : -1;
        }

        protected abstract int compareTerm(Terms.Bucket other);

        public Bucket reduce(List<? extends Bucket> buckets, CacheRecycler cacheRecycler) {
            if (buckets.size() == 1) {
                return buckets.get(0);
            }
            Bucket reduced = null;
            List<InternalAggregations> aggregationsList = new ArrayList<InternalAggregations>(buckets.size());
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, cacheRecycler);
            return reduced;
        }
    }

    protected InternalOrder order;
    protected int requiredSize;
    protected Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;

    protected InternalTerms() {} // for serialization

    protected InternalTerms(String name, InternalOrder order, int requiredSize, Collection<Bucket> buckets) {
        super(name);
        this.order = order;
        this.requiredSize = requiredSize;
        this.buckets = buckets;
    }

    @Override
    public Iterator<Terms.Bucket> iterator() {
        Object o = buckets.iterator();
        return (Iterator<Terms.Bucket>) o;
    }

    @Override
    public Collection<Terms.Bucket> buckets() {
        Object o = buckets;
        return (Collection<Terms.Bucket>) o;
    }

    @Override
    public Terms.Bucket getByTerm(String term) {
        if (bucketMap == null) {
            bucketMap = Maps.newHashMapWithExpectedSize(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey().string(), bucket);
            }
        }
        return bucketMap.get(term);
    }

    @Override
    public InternalTerms reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            InternalTerms terms = (InternalTerms) aggregations.get(0);
            terms.trimExcessEntries();
            return terms;
        }
        InternalTerms reduced = null;

        // TODO: would it be better to use a hppc map and then directly work on the backing array instead of using a PQ?

        Map<Text, List<InternalTerms.Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalTerms terms = (InternalTerms) aggregation;
            if (terms instanceof UnmappedTerms) {
                continue;
            }
            if (reduced == null) {
                reduced = terms;
            }
            if (buckets == null) {
                buckets = new HashMap<Text, List<Bucket>>(terms.buckets.size());
            }
            for (Bucket bucket : terms.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.getKey());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<Bucket>(aggregations.size());
                    buckets.put(bucket.getKey(), existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        if (reduced == null) {
            // there are only unmapped terms, so we just return the first one (no need to reduce)
            return (UnmappedTerms) aggregations.get(0);
        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketPriorityQueue ordered = new BucketPriorityQueue(size, order.comparator());
        for (Map.Entry<Text, List<Bucket>> entry : buckets.entrySet()) {
            List<Bucket> sameTermBuckets = entry.getValue();
            ordered.insertWithOverflow(sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext.cacheRecycler()));
        }
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = (Bucket) ordered.pop();
        }
        reduced.buckets = Arrays.asList(list);
        return reduced;
    }

    protected void trimExcessEntries() {
        if (requiredSize >= buckets.size()) {
            return;
        }

        if (buckets instanceof List) {
            buckets = ((List) buckets).subList(0, requiredSize);
            return;
        }

        int i = 0;
        for (Iterator<Bucket> iter  = buckets.iterator(); iter.hasNext();) {
            iter.next();
            if (i++ >= requiredSize) {
                iter.remove();
            }
        }
    }

}
