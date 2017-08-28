/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;

import java.io.IOException;
import java.util.Objects;

class ProcessingHitExtractor implements HitExtractor {
    static final String NAME = "p";
    private final HitExtractor delegate;
    private final ColumnProcessor processor;

    ProcessingHitExtractor(HitExtractor delegate, ColumnProcessor processor) {
        this.delegate = delegate;
        this.processor = processor;
    }

    ProcessingHitExtractor(StreamInput in) throws IOException {
        delegate = in.readNamedWriteable(HitExtractor.class);
        processor = in.readNamedWriteable(ColumnProcessor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(delegate);
        out.writeNamedWriteable(processor);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    HitExtractor delegate() {
        return delegate;
    }

    ColumnProcessor processor() {
        return processor;
    }

    @Override
    public Object get(SearchHit hit) {
        return processor.apply(delegate.get(hit));
    }

    @Override
    public String innerHitName() {
        return delegate.innerHitName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ProcessingHitExtractor other = (ProcessingHitExtractor) obj;
        return delegate.equals(other.delegate)
                && processor.equals(other.processor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, processor);
    }

    @Override
    public String toString() {
        return processor + "(" + delegate + ")";
    }
}
