/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.processor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;

import java.io.IOException;
import java.util.Objects;

/**
 * Processor wrapping a {@link HitExtractor}, essentially being a source/leaf of a
 * Processor tree.
 */
public class HitExtractorProcessor implements Processor {

    public static final String NAME = "h";

    private final HitExtractor extractor;

    public HitExtractorProcessor(HitExtractor extractor) {
        this.extractor = extractor;
    }

    public HitExtractorProcessor(StreamInput in) throws IOException {
        extractor = in.readNamedWriteable(HitExtractor.class);
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
        if (!(input instanceof SearchHit)) {
            throw new SqlIllegalArgumentException("Expected a SearchHit but received {}", input);
        }
        return extractor.extract((SearchHit) input);
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

        HitExtractorProcessor other = (HitExtractorProcessor) obj;
        return Objects.equals(extractor, other.extractor);
    }

    @Override
    public String toString() {
        return extractor.toString();
    }
}
