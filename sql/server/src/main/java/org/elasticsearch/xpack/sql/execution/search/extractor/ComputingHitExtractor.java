/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.HitExtractorProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import java.io.IOException;
import java.util.Objects;

/**
 * HitExtractor that delegates to a processor. The difference between this class
 * and {@link HitExtractorProcessor} is that the latter is used inside a
 * {@link Processor} tree as a leaf (and thus can effectively parse the
 * {@link SearchHit} while this class is used when scrolling and passing down
 * the results.
 * 
 * In the future, the processor might be used across the board for all columns
 * to reduce API complexity (and keep the {@link HitExtractor} only as an
 * internal implementation detail).
 */
public class ComputingHitExtractor implements HitExtractor {
    static final String NAME = "p";
    private final Processor processor;

    public ComputingHitExtractor(Processor processor) {
        this.processor = processor;
    }

    ComputingHitExtractor(StreamInput in) throws IOException {
        processor = in.readNamedWriteable(Processor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(processor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Processor processor() {
        return processor;
    }

    @Override
    public Object get(SearchHit hit) {
        return processor.process(hit);
    }

    @Override
    public String innerHitName() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ComputingHitExtractor other = (ComputingHitExtractor) obj;
        return processor.equals(other.processor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processor);
    }

    @Override
    public String toString() {
        return processor.toString();
    }
}