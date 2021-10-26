/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.execution.search.extractor;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.io.IOException;

/**
 * Returns the implicit Elasticsearch tiebreaker value associated with a PIT request for every search hit against which it is run.
 */
public class ImplicitTiebreakerHitExtractor implements HitExtractor {

    public static final HitExtractor INSTANCE = new ImplicitTiebreakerHitExtractor();
    static final String NAME = "tb";

    private ImplicitTiebreakerHitExtractor() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException { }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object extract(SearchHit hit) {
        Object[] sortValues = hit.getRawSortValues();
        if (sortValues.length == 0) {
            throw new EqlIllegalArgumentException("Expected at least one sorting value in the search hit, but got none");
        }
        return sortValues[sortValues.length - 1];
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
        return true;
    }

    @Override
    public int hashCode() {
        return ImplicitTiebreakerHitExtractor.class.hashCode();
    }

    @Override
    public String toString() {
        return "_shard_doc";
    }
}
