/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the running the significant terms aggregation on a numeric field.
 */
public class SignificantLongTerms extends InternalMappedSignificantTerms<SignificantLongTerms, SignificantLongTerms.Bucket> {
    public static final String NAME = "siglterms";

    public static class Bucket extends InternalSignificantTerms.Bucket<Bucket> {

        long term;

        public Bucket(long subsetDf, long supersetDf, long term, InternalAggregations aggregations, DocValueFormat format, double score) {
            super(subsetDf, supersetDf, aggregations, format);
            this.term = term;
            this.score = score;
        }

        Bucket(StreamInput in, DocValueFormat format) throws IOException {
            super(format);
            subsetDf = in.readVLong();
            supersetDf = in.readVLong();
            term = in.readLong();
            score = in.readDouble();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(subsetDf);
            out.writeVLong(supersetDf);
            out.writeLong(term);
            out.writeDouble(getSignificanceScore());
            aggregations.writeTo(out);
        }

        @Override
        public Object getKey() {
            return term;
        }

        @Override
        public String getKeyAsString() {
            return format.format(term).toString();
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            builder.field(CommonFields.KEY.getPreferredName(), term);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term));
            }
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(term, ((Bucket) obj).term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), term);
        }
    }

    public SignificantLongTerms(
        String name,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata,
        DocValueFormat format,
        long subsetSize,
        long supersetSize,
        SignificanceHeuristic significanceHeuristic,
        List<Bucket> buckets
    ) {
        super(name, requiredSize, minDocCount, metadata, format, subsetSize, supersetSize, significanceHeuristic, buckets);
    }

    /**
     * Read from a stream.
     */
    public SignificantLongTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SignificantLongTerms create(List<SignificantLongTerms.Bucket> buckets) {
        return new SignificantLongTerms(
            name,
            requiredSize,
            minDocCount,
            metadata,
            format,
            subsetSize,
            supersetSize,
            significanceHeuristic,
            buckets
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, SignificantLongTerms.Bucket prototype) {
        return new Bucket(prototype.subsetDf, prototype.supersetDf, prototype.term, aggregations, prototype.format, prototype.score);
    }

    @Override
    protected SignificantLongTerms create(long subsetSize, long supersetSize, List<Bucket> buckets) {
        return new SignificantLongTerms(
            getName(),
            requiredSize,
            minDocCount,
            getMetadata(),
            format,
            subsetSize,
            supersetSize,
            significanceHeuristic,
            buckets
        );
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    Bucket createBucket(long subsetDf, long supersetDf, InternalAggregations aggregations, SignificantLongTerms.Bucket prototype) {
        return new Bucket(subsetDf, supersetDf, prototype.term, aggregations, format, prototype.score);
    }
}
