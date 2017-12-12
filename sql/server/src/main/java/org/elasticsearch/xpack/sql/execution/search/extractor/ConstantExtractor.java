/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.Objects;

/**
 * Returns the a constant for every search hit against which it is run.
 */
public class ConstantExtractor implements HitExtractor {
    static final String NAME = "c";
    private final Object constant;

    public ConstantExtractor(Object constant) {
        this.constant = constant;
    }

    ConstantExtractor(StreamInput in) throws IOException {
        constant = in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(constant);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object get(SearchHit hit) {
        return constant;
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
        ConstantExtractor other = (ConstantExtractor) obj;
        return Objects.equals(constant, other.constant);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(constant);
    }

    @Override
    public String toString() {
        return "^" + constant;
    }
}