/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ObjectArrayPriorityQueue;
import org.elasticsearch.common.util.ObjectObjectPagedHashMap;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.DelayedBucketReducer;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class InternalComposite extends InternalMultiBucketAggregation<InternalComposite, InternalComposite.InternalBucket>
    implements
        CompositeAggregation {

    private final int size;
    private final List<InternalBucket> buckets;
    private final CompositeKey afterKey;
    private final int[] reverseMuls;
    private final MissingOrder[] missingOrders;
    private final List<String> sourceNames;
    private final List<DocValueFormat> formats;

    private final boolean earlyTerminated;

    InternalComposite(
        String name,
        int size,
        List<String> sourceNames,
        List<DocValueFormat> formats,
        List<InternalBucket> buckets,
        CompositeKey afterKey,
        int[] reverseMuls,
        MissingOrder[] missingOrders,
        boolean earlyTerminated,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.sourceNames = sourceNames;
        this.formats = formats;
        this.buckets = buckets;
        this.afterKey = afterKey;
        this.size = size;
        this.reverseMuls = reverseMuls;
        this.missingOrders = missingOrders;
        this.earlyTerminated = earlyTerminated;
    }

    /**
     * Checks that the afterKey formatting does not result in loss of information
     *
     * Only called when a new InternalComposite() is built after a reduce.  We can't validate afterKeys from
     * InternalComposites built from a StreamInput because they may be coming from nodes that do not
     * do validation, and errors thrown during StreamInput deserialization can kill a node.  However,
     * InternalComposites that come from remote nodes will always be reduced on the co-ordinator, and
     * this will always create a new InternalComposite by calling the standard constructor.
     */
    private void validateAfterKey() {
        if (afterKey != null) {
            if (this.formats.size() != this.afterKey.size()) {
                throw new IllegalArgumentException("Cannot format afterkey [" + this.afterKey + "] - wrong number of formats");
            }
            for (int i = 0; i < this.afterKey.size(); i++) {
                formatObject(this.afterKey.get(i), this.formats.get(i));
            }
        }
    }

    public InternalComposite(StreamInput in) throws IOException {
        super(in);
        this.size = in.readVInt();
        this.sourceNames = in.readStringCollectionAsList();
        this.formats = new ArrayList<>(sourceNames.size());
        for (int i = 0; i < sourceNames.size(); i++) {
            formats.add(in.readNamedWriteable(DocValueFormat.class));
        }
        this.reverseMuls = in.readIntArray();
        this.missingOrders = in.readArray(MissingOrder::readFromStream, MissingOrder[]::new);
        this.buckets = in.readCollectionAsList((input) -> new InternalBucket(input, sourceNames, formats, reverseMuls, missingOrders));
        this.afterKey = in.readOptionalWriteable(CompositeKey::new);
        this.earlyTerminated = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        out.writeStringCollection(sourceNames);
        for (DocValueFormat format : formats) {
            out.writeNamedWriteable(format);
        }
        out.writeIntArray(reverseMuls);
        out.writeArray(missingOrders);
        out.writeCollection(buckets);
        out.writeOptionalWriteable(afterKey);
        out.writeBoolean(earlyTerminated);
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
    public InternalComposite create(List<InternalBucket> newBuckets) {
        /**
         * This is used by pipeline aggregations to filter/remove buckets so we
         * keep the <code>afterKey</code> of the original aggregation in order
         * to be able to retrieve the next page even if all buckets have been filtered.
         */
        return new InternalComposite(
            name,
            size,
            sourceNames,
            formats,
            newBuckets,
            afterKey,
            reverseMuls,
            missingOrders,
            earlyTerminated,
            getMetadata()
        );
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(
            prototype.sourceNames,
            prototype.formats,
            prototype.key,
            prototype.reverseMuls,
            prototype.missingOrders,
            prototype.docCount,
            aggregations
        );
    }

    public int getSize() {
        return size;
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    /**
     * The formats used when writing the keys. Package private for testing.
     */
    List<DocValueFormat> getFormats() {
        return formats;
    }

    @Override
    public Map<String, Object> afterKey() {
        if (afterKey != null) {
            return new ArrayMap(sourceNames, formats, afterKey.values());
        }
        return null;
    }

    // Visible for tests
    boolean isTerminatedEarly() {
        return earlyTerminated;
    }

    // Visible for tests
    int[] getReverseMuls() {
        return reverseMuls;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final BucketsQueue queue = new BucketsQueue(reduceContext);
            boolean earlyTerminated = false;

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalComposite sortedAgg = (InternalComposite) aggregation;
                earlyTerminated |= sortedAgg.earlyTerminated;
                for (InternalBucket bucket : sortedAgg.getBuckets()) {
                    if (queue.add(bucket) == false) {
                        // if the bucket is not competitive, we can break
                        // because incoming buckets are sorted
                        break;
                    }
                }
            }

            @Override
            public InternalAggregation get() {
                final List<InternalBucket> result = queue.get();
                List<DocValueFormat> reducedFormats = formats;
                CompositeKey lastKey = null;
                if (result.isEmpty() == false) {
                    InternalBucket lastBucket = result.get(result.size() - 1);
                    /* Attach the formats from the last bucket to the reduced composite
                     * so that we can properly format the after key. */
                    reducedFormats = lastBucket.formats;
                    lastKey = lastBucket.getRawKey();
                }
                reduceContext.consumeBucketsAndMaybeBreak(result.size());
                InternalComposite reduced = new InternalComposite(
                    name,
                    getSize(),
                    sourceNames,
                    reducedFormats,
                    result,
                    lastKey,
                    reverseMuls,
                    missingOrders,
                    earlyTerminated,
                    metadata
                );
                reduced.validateAfterKey();
                return reduced;
            }

            @Override
            public void close() {
                Releasables.close(queue);
            }
        };
    }

    private class BucketsQueue implements Releasable {
        private final ObjectObjectPagedHashMap<Object, DelayedBucketReducer<InternalBucket>> bucketReducers;
        private final ObjectArrayPriorityQueue<InternalBucket> queue;
        private final AggregationReduceContext reduceContext;

        private BucketsQueue(AggregationReduceContext reduceContext) {
            this.reduceContext = reduceContext;
            bucketReducers = new ObjectObjectPagedHashMap<>(getSize(), reduceContext.bigArrays());
            queue = new ObjectArrayPriorityQueue<>(getSize(), reduceContext.bigArrays()) {
                @Override
                protected boolean lessThan(InternalBucket a, InternalBucket b) {
                    return b.compareKey(a) < 0;
                }
            };
        }

        /** adds a bucket to the queue. Return false if the bucket is not competitive, otherwise true.*/
        boolean add(InternalBucket bucket) {
            DelayedBucketReducer<InternalBucket> delayed = bucketReducers.get(bucket.key);
            if (delayed == null) {
                final InternalBucket out = queue.insertWithOverflow(bucket);
                if (out == null) {
                    // bucket is added
                    delayed = new DelayedBucketReducer<>(bucket, reduceContext);
                } else if (out == bucket) {
                    // bucket is not competitive
                    return false;
                } else {
                    // bucket replaces existing bucket
                    delayed = bucketReducers.remove(out.key);
                    assert delayed != null;
                    delayed.reset(bucket);
                }
                bucketReducers.put(bucket.key, delayed);
            }
            delayed.accept(bucket);
            return true;
        }

        /** Return the list of reduced buckets */
        List<InternalBucket> get() {
            final int bucketsSize = (int) bucketReducers.size();
            final InternalBucket[] result = new InternalBucket[bucketsSize];
            for (int i = bucketsSize - 1; i >= 0; i--) {
                final InternalBucket bucket = queue.pop();
                assert bucket != null;
                /* Use the formats from the bucket because they'll be right to format
                 * the key. The formats on the InternalComposite doing the reducing are
                 * just whatever formats make sense for *its* index. This can be real
                 * trouble when the index doing the reducing is unmapped. */
                final var reducedFormats = bucket.formats;
                final DelayedBucketReducer<InternalBucket> reducer = Objects.requireNonNull(bucketReducers.get(bucket.key));
                result[i] = new InternalBucket(
                    sourceNames,
                    reducedFormats,
                    bucket.key,
                    reverseMuls,
                    missingOrders,
                    reducer.getDocCount(),
                    reducer.getAggregations()
                );
            }
            return List.of(result);
        }

        @Override
        public void close() {
            Releasables.close(bucketReducers, queue);
        }
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalComposite(
            name,
            size,
            sourceNames,
            buckets.isEmpty() ? formats : buckets.get(buckets.size() - 1).formats,
            buckets.stream().map(b -> b.finalizeSampling(samplingContext)).toList(),
            buckets.isEmpty() ? afterKey : buckets.get(buckets.size() - 1).getRawKey(),
            reverseMuls,
            missingOrders,
            earlyTerminated,
            metadata
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalComposite that = (InternalComposite) obj;
        return Objects.equals(size, that.size)
            && Objects.equals(buckets, that.buckets)
            && Objects.equals(afterKey, that.afterKey)
            && Arrays.equals(reverseMuls, that.reverseMuls)
            && Arrays.equals(missingOrders, that.missingOrders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), size, buckets, afterKey, Arrays.hashCode(reverseMuls), Arrays.hashCode(missingOrders));
    }

    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket
        implements
            CompositeAggregation.Bucket,
            KeyComparable<InternalBucket> {

        private final CompositeKey key;
        private final long docCount;
        private final InternalAggregations aggregations;
        private final transient int[] reverseMuls;
        private final transient MissingOrder[] missingOrders;
        private final transient List<String> sourceNames;
        private final transient List<DocValueFormat> formats;

        InternalBucket(
            List<String> sourceNames,
            List<DocValueFormat> formats,
            CompositeKey key,
            int[] reverseMuls,
            MissingOrder[] missingOrders,
            long docCount,
            InternalAggregations aggregations
        ) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.reverseMuls = reverseMuls;
            this.missingOrders = missingOrders;
            this.sourceNames = sourceNames;
            this.formats = formats;
        }

        InternalBucket(
            StreamInput in,
            List<String> sourceNames,
            List<DocValueFormat> formats,
            int[] reverseMuls,
            MissingOrder[] missingOrders
        ) throws IOException {
            this.key = new CompositeKey(in);
            this.docCount = in.readVLong();
            this.aggregations = InternalAggregations.readFrom(in);
            this.reverseMuls = reverseMuls;
            this.missingOrders = missingOrders;
            this.sourceNames = sourceNames;
            this.formats = formats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            key.writeTo(out);
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
            // returns the formatted key in a map
            return new ArrayMap(sourceNames, formats, key.values());
        }

        // get the raw key (without formatting to preserve the natural order).
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
                builder.append(formatObject(key.get(i), formats.get(i)));
            }
            builder.append('}');
            return builder.toString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        /**
         * The formats used when writing the keys. Package private for testing.
         */
        List<DocValueFormat> getFormats() {
            return formats;
        }

        @Override
        public int compareKey(InternalBucket other) {
            for (int i = 0; i < key.size(); i++) {
                if (key.get(i) == null) {
                    if (other.key.get(i) == null) {
                        continue;
                    }
                    return -1 * missingOrders[i].compareAnyValueToMissing(reverseMuls[i]);
                } else if (other.key.get(i) == null) {
                    return missingOrders[i].compareAnyValueToMissing(reverseMuls[i]);
                }
                assert key.get(i).getClass() == other.key.get(i).getClass();
                @SuppressWarnings("unchecked")
                int cmp = key.get(i).compareTo(other.key.get(i)) * reverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /**
             * See {@link CompositeAggregation#bucketToXContent}
             */
            throw new UnsupportedOperationException("not implemented");
        }

        InternalBucket finalizeSampling(SamplingContext samplingContext) {
            return new InternalBucket(
                sourceNames,
                formats,
                key,
                reverseMuls,
                missingOrders,
                samplingContext.scaleUp(docCount),
                InternalAggregations.finalizeSampling(aggregations, samplingContext)
            );
        }
    }

    /**
     * Format <code>obj</code> using the provided {@link DocValueFormat}.
     * If the format is equals to {@link DocValueFormat#RAW}, the object is returned as is
     * for numbers and a string for {@link BytesRef}s.
     *
     * This method will then attempt to parse the formatted value using the specified format,
     * and throw an IllegalArgumentException if parsing fails.  This in turn prevents us from
     * returning an after_key which we can't subsequently parse into the original value.
     */
    static Object formatObject(Object obj, DocValueFormat format) {
        if (obj == null) {
            return null;
        }
        Object formatted = obj;
        Object parsed;
        if (obj.getClass() == BytesRef.class && format == DocValueFormat.TIME_SERIES_ID) {
            BytesRef value = (BytesRef) obj;
            // NOTE: formatting a tsid returns a Base64 encoding of the tsid BytesRef which we cannot use to get back the original tsid
            formatted = format.format(value);
            parsed = format.parseBytesRef(value);
            // NOTE: we cannot parse the Base64 encoding representation of the tsid and get back the original BytesRef
            if (parsed.equals(obj) == false) {
                throw new IllegalArgumentException(
                    "Format ["
                        + format
                        + "] created output it couldn't parse for value ["
                        + obj
                        + "] "
                        + "of type ["
                        + obj.getClass()
                        + "]. formatted value: ["
                        + formatted
                        + "("
                        + parsed.getClass()
                        + ")]"
                );
            }
        }
        if (obj.getClass() == BytesRef.class && format != DocValueFormat.TIME_SERIES_ID) {
            BytesRef value = (BytesRef) obj;
            if (format == DocValueFormat.RAW) {
                formatted = value.utf8ToString();
            } else {
                formatted = format.format(value);
            }
            parsed = format.parseBytesRef(formatted.toString());
            if (parsed.equals(obj) == false) {
                throw new IllegalArgumentException(
                    "Format ["
                        + format
                        + "] created output it couldn't parse for value ["
                        + obj
                        + "] "
                        + "of type ["
                        + obj.getClass()
                        + "]. parsed value: ["
                        + parsed
                        + "("
                        + parsed.getClass()
                        + ")]"
                );
            }
        } else if (obj.getClass() == Long.class) {
            long value = (long) obj;
            if (format == DocValueFormat.RAW) {
                formatted = value;
            } else {
                formatted = format.format(value);
            }
            parsed = format.parseLong(formatted.toString(), false, () -> {
                throw new UnsupportedOperationException("Using now() is not supported in after keys");
            });
            if (parsed.equals(((Number) obj).longValue()) == false) {
                throw new IllegalArgumentException(
                    "Format ["
                        + format
                        + "] created output it couldn't parse for value ["
                        + obj
                        + "] "
                        + "of type ["
                        + obj.getClass()
                        + "]. parsed value: ["
                        + parsed
                        + "("
                        + parsed.getClass()
                        + ")]"
                );
            }
        } else if (obj.getClass() == Double.class) {
            double value = (double) obj;
            if (format == DocValueFormat.RAW) {
                formatted = value;
            } else {
                formatted = format.format(value);
            }
            parsed = format.parseDouble(formatted.toString(), false, () -> {
                throw new UnsupportedOperationException("Using now() is not supported in after keys");
            });
            if (parsed.equals(((Number) obj).doubleValue()) == false) {
                throw new IllegalArgumentException(
                    "Format ["
                        + format
                        + "] created output it couldn't parse for value ["
                        + obj
                        + "] "
                        + "of type ["
                        + obj.getClass()
                        + "]. parsed value: ["
                        + parsed
                        + "("
                        + parsed.getClass()
                        + ")]"
                );
            }
        }
        return formatted;
    }

    static class ArrayMap extends AbstractMap<String, Object> implements Comparable<ArrayMap> {
        final List<String> keys;
        final Comparable<?>[] values;
        final List<DocValueFormat> formats;

        ArrayMap(List<String> keys, List<DocValueFormat> formats, Comparable<?>[] values) {
            assert keys.size() == values.length && keys.size() == formats.size();
            this.keys = keys;
            this.formats = formats;
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
                    return formatObject(values[i], formats.get(i));
                }
            }
            return null;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<>() {
                        int pos = 0;

                        @Override
                        public boolean hasNext() {
                            return pos < values.length;
                        }

                        @Override
                        public Entry<String, Object> next() {
                            SimpleEntry<String, Object> entry = new SimpleEntry<>(
                                keys.get(pos),
                                formatObject(values[pos], formats.get(pos))
                            );
                            ++pos;
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

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public int compareTo(ArrayMap that) {
            if (that == this) {
                return 0;
            }

            int idx = 0;
            int max = Math.min(this.keys.size(), that.keys.size());
            while (idx < max) {
                int compare = compareNullables(keys.get(idx), that.keys.get(idx));
                if (compare == 0) {
                    compare = compareNullables((Comparable) values[idx], (Comparable) that.values[idx]);
                }
                if (compare != 0) {
                    return compare;
                }
                idx++;
            }
            if (idx < keys.size()) {
                return 1;
            }
            if (idx < that.keys.size()) {
                return -1;
            }
            return 0;
        }
    }

    private static <T extends Comparable<T>> int compareNullables(T a, T b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        return a.compareTo(b);
    }
}
