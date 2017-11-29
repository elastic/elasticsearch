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

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

public class InternalComposite
    extends InternalMultiBucketAggregation<InternalComposite, InternalComposite.InternalBucket> implements CompositeAggregation {

    private final int size;
    private final List<InternalBucket> buckets;
    private final int[] reverseMuls;
    private final List<String> sourceNames;

    InternalComposite(String name, int size, List<String> sourceNames, List<InternalBucket> buckets, int[] reverseMuls,
                      List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sourceNames = sourceNames;
        this.buckets = buckets;
        this.size = size;
        this.reverseMuls = reverseMuls;
    }

    public InternalComposite(StreamInput in) throws IOException {
        super(in);
        this.size = in.readVInt();
        this.sourceNames = in.readList(StreamInput::readString);
        this.reverseMuls = in.readIntArray();
        this.buckets = in.readList((input) -> new InternalBucket(input, sourceNames, reverseMuls));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        out.writeStringList(sourceNames);
        out.writeIntArray(reverseMuls);
        out.writeList(buckets);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return CompositeAggregation.toXContentFragment(this, builder, params);
    }

    @Override
    public String getWriteableName() {
        return CompositeAggregationBuilder.NAME;
    }

    @Override
    public InternalComposite create(List<InternalBucket> buckets) {
        return new InternalComposite(name, size, sourceNames, buckets, reverseMuls, pipelineAggregators(), getMetaData());
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.sourceNames, prototype.key, prototype.reverseMuls, prototype.docCount, aggregations);
    }

    public int getSize() {
        return size;
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    @Override
    public Map<String, Object> afterKey() {
        return buckets.size() > 0 ? buckets.get(buckets.size()-1).getKey() : null;
    }

    // Visible for tests
    int[] getReverseMuls() {
        return reverseMuls;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        PriorityQueue<BucketIterator> pq = new PriorityQueue<>(aggregations.size());
        for (InternalAggregation agg : aggregations) {
            InternalComposite sortedAgg = (InternalComposite) agg;
            BucketIterator it = new BucketIterator(sortedAgg.buckets);
            if (it.next() != null) {
                pq.add(it);
            }
        }
        InternalBucket lastBucket = null;
        List<InternalBucket> buckets = new ArrayList<>();
        List<InternalBucket> result = new ArrayList<>();
        while (pq.size() > 0) {
            BucketIterator bucketIt = pq.poll();
            if (lastBucket != null && bucketIt.current.compareKey(lastBucket) != 0) {
                InternalBucket reduceBucket = buckets.get(0).reduce(buckets, reduceContext);
                buckets.clear();
                result.add(reduceBucket);
                if (result.size() >= size) {
                    break;
                }
            }
            lastBucket = bucketIt.current;
            buckets.add(bucketIt.current);
            if (bucketIt.next() != null) {
                pq.add(bucketIt);
            }
        }
        if (buckets.size() > 0) {
            InternalBucket reduceBucket = buckets.get(0).reduce(buckets, reduceContext);
            result.add(reduceBucket);
        }
        return new InternalComposite(name, size, sourceNames, result, reverseMuls, pipelineAggregators(), metaData);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalComposite that = (InternalComposite) obj;
        return Objects.equals(size, that.size) &&
            Objects.equals(buckets, that.buckets) &&
            Arrays.equals(reverseMuls, that.reverseMuls);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(size, buckets, Arrays.hashCode(reverseMuls));
    }

    private static class BucketIterator implements Comparable<BucketIterator> {
        final Iterator<InternalBucket> it;
        InternalBucket current;

        private BucketIterator(List<InternalBucket> buckets) {
            this.it = buckets.iterator();
        }

        @Override
        public int compareTo(BucketIterator other) {
            return current.compareKey(other.current);
        }

        InternalBucket next() {
            return current = it.hasNext() ? it.next() : null;
        }
    }

    static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket
            implements CompositeAggregation.Bucket, KeyComparable<InternalBucket> {

        private final CompositeKey key;
        private final long docCount;
        private final InternalAggregations aggregations;
        private final transient int[] reverseMuls;
        private final transient List<String> sourceNames;


        InternalBucket(List<String> sourceNames, CompositeKey key, int[] reverseMuls, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.reverseMuls = reverseMuls;
            this.sourceNames = sourceNames;
        }

        @SuppressWarnings("unchecked")
        InternalBucket(StreamInput in, List<String> sourceNames, int[] reverseMuls) throws IOException {
            final Comparable<?>[] values = new Comparable<?>[in.readVInt()];
            for (int i = 0; i < values.length; i++) {
                values[i] = (Comparable<?>) in.readGenericValue();
            }
            this.key = new CompositeKey(values);
            this.docCount = in.readVLong();
            this.aggregations = InternalAggregations.readAggregations(in);
            this.reverseMuls = reverseMuls;
            this.sourceNames = sourceNames;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(key.size());
            for (int i = 0; i < key.size(); i++) {
                out.writeGenericValue(key.get(i));
            }
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, key, aggregations);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            InternalBucket that = (InternalBucket) obj;
            return Objects.equals(docCount, that.docCount)
                && Objects.equals(key, that.key)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public Map<String, Object> getKey() {
            return new ArrayMap(sourceNames, key.values());
        }

        // visible for testing
        CompositeKey getRawKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            for (int i = 0; i < key.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(sourceNames.get(i));
                builder.append('=');
                builder.append(formatObject(key.get(i)));
            }
            builder.append('}');
            return builder.toString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        InternalBucket reduce(List<InternalBucket> buckets, ReduceContext reduceContext) {
            List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (InternalBucket bucket : buckets) {
                docCount += bucket.docCount;
                aggregations.add(bucket.aggregations);
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregations, reduceContext);
            return new InternalBucket(sourceNames, key, reverseMuls, docCount, aggs);
        }

        @Override
        public int compareKey(InternalBucket other) {
            for (int i = 0; i < key.size(); i++) {
                assert key.get(i).getClass() == other.key.get(i).getClass();
                @SuppressWarnings("unchecked")
                int cmp = ((Comparable) key.get(i)).compareTo(other.key.get(i)) * reverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /**
             * See {@link CompositeAggregation#bucketToXContentFragment}
             */
            throw new UnsupportedOperationException("not implemented");
        }
    }

    static Object formatObject(Object obj) {
        if (obj instanceof BytesRef) {
            return ((BytesRef) obj).utf8ToString();
        }
        return obj;
    }

    private static class ArrayMap extends AbstractMap<String, Object> {
        final List<String> keys;
        final Object[] values;

        ArrayMap(List<String> keys, Object[] values) {
            assert keys.size() == values.length;
            this.keys = keys;
            this.values = values;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Object get(Object key) {
            for (int i = 0; i < keys.size(); i++) {
                if (key.equals(keys.get(i))) {
                    return formatObject(values[i]);
                }
            }
            return null;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<Entry<String, Object>>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<Entry<String, Object>>() {
                        int pos = 0;
                        @Override
                        public boolean hasNext() {
                            return pos < values.length;
                        }

                        @Override
                        public Entry<String, Object> next() {
                            SimpleEntry<String, Object> entry =
                                new SimpleEntry<>(keys.get(pos), formatObject(values[pos]));
                            ++ pos;
                            return entry;
                        }
                    };
                }

                @Override
                public int size() {
                    return keys.size();
                }
            };
        }
    }
}
