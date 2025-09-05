/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

public abstract class EsqlQueryRequest extends LegacyActionRequest {

    protected EsqlQueryRequest() {}

    protected EsqlQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    public abstract String query();

    public abstract QueryBuilder filter();
}
