/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.MatchQueryBuilder;

import java.io.IOException;

public class InterceptedMatchQueryBuilder extends InterceptedQueryBuilder<MatchQueryBuilder> {
    public static final String NAME = "intercepted_match";

    public InterceptedMatchQueryBuilder(MatchQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Class<MatchQueryBuilder> originalQueryClass() {
        return MatchQueryBuilder.class;
    }
}
