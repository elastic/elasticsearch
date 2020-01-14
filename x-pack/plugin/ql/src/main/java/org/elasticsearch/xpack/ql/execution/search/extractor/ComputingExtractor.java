/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.xpack.ql.expression.gen.processor.HitExtractorProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;

import java.io.IOException;
import java.util.Objects;

/**
 * Hit/BucketExtractor that delegates to a processor. The difference between this class
 * and {@link HitExtractorProcessor} is that the latter is used inside a
 * {@link Processor} tree as a leaf (and thus can effectively parse the
 * {@link SearchHit} while this class is used when scrolling and passing down
 * the results.
 *
 * In the future, the processor might be used across the board for all columns
 * to reduce API complexity (and keep the {@link HitExtractor} only as an
 * internal implementation detail).
 */
public class ComputingExtractor implements HitExtractor, BucketExtractor {
    /**
     * Stands for {@code comPuting}. We try to use short names for {@link HitExtractor}s
     * to save a few bytes when when we send them back to the user.
     */
    static final String NAME = "p";
    private final Processor processor;
    private final String hitName;

    public ComputingExtractor(Processor processor) {
        this(processor, null);
    }

    public ComputingExtractor(Processor processor, String hitName) {
        this.processor = processor;
        this.hitName = hitName;
    }

    // Visibility required for tests
    public ComputingExtractor(StreamInput in) throws IOException {
        processor = in.readNamedWriteable(Processor.class);
        hitName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(processor);
        out.writeOptionalString(hitName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Processor processor() {
        return processor;
    }

    public Object extract(Object input) {
        return processor.process(input);
    }
    
    @Override
    public Object extract(Bucket bucket) {
        return processor.process(bucket);
    }

    @Override
    public Object extract(SearchHit hit) {
        return processor.process(hit);
    }

    @Override
    public String hitName() {
        return hitName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ComputingExtractor other = (ComputingExtractor) obj;
        return Objects.equals(processor, other.processor)
                && Objects.equals(hitName, other.hitName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processor, hitName);
    }

    @Override
    public String toString() {
        return processor.toString();
    }
}