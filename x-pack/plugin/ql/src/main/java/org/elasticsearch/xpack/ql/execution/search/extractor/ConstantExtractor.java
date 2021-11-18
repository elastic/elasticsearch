/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

import java.io.IOException;
import java.util.Objects;

/**
 * Returns the a constant for every search hit against which it is run.
 */
public class ConstantExtractor implements HitExtractor, BucketExtractor {
    /**
     * Stands for {@code constant}. We try to use short names for {@link HitExtractor}s
     * to save a few bytes when when we send them back to the user.
     */
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
    public Object extract(SearchHit hit) {
        return constant;
    }

    @Override
    public Object extract(Bucket bucket) {
        return constant;
    }

    @Override
    public String hitName() {
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
