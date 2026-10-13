/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Result of the {@link TermsAggregator} when the field is a String.
 */
public class StringTerms extends InternalMappedTerms<StringTerms, StringTerms.Bucket> {
    public static final String NAME = "sterms";

    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        BytesRef termBytes;

        public Bucket(
            BytesRef term,
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat format
        ) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.termBytes = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            termBytes = in.readBytesRef();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
        }

        @Override
        public Object getKey() {
            return getKeyAsString();
        }

        public BytesRef getTermBytes() {
            return termBytes;
        }

        public void setTermBytes(BytesRef termBytes) {
            this.termBytes = termBytes;
        }

        // this method is needed for scripted numeric aggs
        @Override
        public Number getKeyAsNumber() {
            /*
             * If the term is a long greater than 2^52 then parsing as a double would lose accuracy. Therefore, we first parse as a long and
             * if this fails then we attempt to parse the term as a double.
             */
            try {
                return Long.parseLong(termBytes.utf8ToString());
            } catch (final NumberFormatException ignored) {
                return Double.parseDouble(termBytes.utf8ToString());
            }
        }

        @Override
        public String getKeyAsString() {
            return format.format(termBytes).toString();
        }

        @Override
        public int compareKey(Bucket other) {
            return termBytes.compareTo(other.termBytes);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            if (format == DocValueFormat.TIME_SERIES_ID) {
                return builder.field(CommonFields.KEY.getPreferredName(), format.format(termBytes));
            }
            return builder.field(CommonFields.KEY.getPreferredName(), getKeyAsString());
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(termBytes, ((Bucket) obj).termBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), termBytes);
        }
    }

    public StringTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata,
        DocValueFormat format,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        List<Bucket> buckets,
        Long docCountError
    ) {
        super(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    /**
     * Read from a stream.
     */
    public StringTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    /**
     * Reduces with the key format of the first result that has buckets, so that results without buckets (for example from an
     * empty index) do not decide how the merged keys are rendered. {@code keyword} and {@code ip} fields both produce
     * {@link StringTerms} and only differ by their {@link DocValueFormat}, so results with buckets and different formats come
     * from a field mapped with incompatible types in different indices and fail the reduction.
     */
    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            /** Results accepted while no result with buckets has been seen yet. */
            private final List<InternalAggregation> pending = new ArrayList<>();
            /** Reduces with the format of the first result that has buckets; {@code null} until that result arrives. */
            private AggregatorReducer delegate = null;
            private DocValueFormat keyFormat = null;

            @Override
            public void accept(InternalAggregation aggregation) {
                if (aggregation instanceof StringTerms terms && terms.getBuckets().isEmpty() == false) {
                    if (delegate == null) {
                        keyFormat = terms.format;
                        delegate = startDelegate(terms);
                    } else if (haveSameFormat(keyFormat, terms.format) == false) {
                        throw AggregationErrors.reduceTypeMismatch(name, Optional.empty());
                    }
                }
                if (delegate == null) {
                    pending.add(aggregation);
                } else {
                    delegate.accept(aggregation);
                }
            }

            @Override
            public InternalAggregation get() {
                if (delegate == null) {
                    delegate = startDelegate(StringTerms.this);
                }
                return delegate.get();
            }

            private AggregatorReducer startDelegate(StringTerms formatSource) {
                AggregatorReducer reducer = formatSource.termsAggregationReducer(reduceContext, size);
                for (InternalAggregation aggregation : pending) {
                    reducer.accept(aggregation);
                }
                pending.clear();
                return reducer;
            }

            @Override
            public void close() {
                Releasables.close(delegate);
            }
        };
    }

    /**
     * Each shard result carries its own deserialized format instance, so formats are compared by name. The formats a field can
     * give {@link StringTerms} keys ({@code raw}, {@code ip}, {@code binary}, {@code version}, ...) are identified by their name.
     */
    private static boolean haveSameFormat(DocValueFormat a, DocValueFormat b) {
        return a.getWriteableName().equals(b.getWriteableName());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public StringTerms create(List<Bucket> buckets) {
        return new StringTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(
            prototype.termBytes,
            prototype.docCount,
            aggregations,
            showTermDocCountError,
            prototype.getDocCountError(),
            prototype.format
        );
    }

    @Override
    protected Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, StringTerms.Bucket prototype) {
        return new Bucket(prototype.termBytes, docCount, aggs, showTermDocCountError, docCountError, format);
    }

    @Override
    protected StringTerms create(String name, List<Bucket> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount) {
        return new StringTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            getMetadata(),
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }
}
