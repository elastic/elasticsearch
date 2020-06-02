/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.gen.processor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;

import java.io.IOException;
import java.util.Objects;

/**
 * Processor wrapping an {@link BucketExtractor}, essentially being a source/leaf of a
 * Processor tree.
 */
public class BucketExtractorProcessor implements Processor {

    public static final String NAME = "a";

    private final BucketExtractor extractor;

    public BucketExtractorProcessor(BucketExtractor extractor) {
        this.extractor = extractor;
    }

    public BucketExtractorProcessor(StreamInput in) throws IOException {
        extractor = in.readNamedWriteable(BucketExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(extractor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object input) {
        if (!(input instanceof Bucket)) {
            throw new QlIllegalArgumentException("Expected an agg bucket but received {}", input);
        }
        return extractor.extract((Bucket) input);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extractor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BucketExtractorProcessor other = (BucketExtractorProcessor) obj;
        return Objects.equals(extractor, other.extractor);
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
