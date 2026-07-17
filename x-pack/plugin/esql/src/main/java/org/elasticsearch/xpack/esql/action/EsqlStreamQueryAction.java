/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.PageStreamPublisher;

import java.io.IOException;
import java.util.List;

/**
 * Action type for the streaming ES|QL query endpoint ({@code POST /_query/stream}).
 * Unlike {@link EsqlQueryAction}, this action responds before compute finishes, delivering
 * a {@link PageStreamPublisher} that the REST layer subscribes to for incremental NDJSON output.
 */
public class EsqlStreamQueryAction extends ActionType<EsqlStreamQueryAction.Response> {

    public static final EsqlStreamQueryAction INSTANCE = new EsqlStreamQueryAction();
    public static final String NAME = "indices:data/read/esql/stream";

    private EsqlStreamQueryAction() {
        super(NAME);
    }

    /**
     * Response for the streaming ES|QL query action. Carries the schema (available after analysis)
     * and a publisher of result pages (produced incrementally as compute runs).
     */
    public static class Response extends ActionResponse {

        private final List<ColumnInfoImpl> columns;
        private final PageStreamPublisher publisher;

        public Response(List<ColumnInfoImpl> columns, PageStreamPublisher publisher) {
            this.columns = columns;
            this.publisher = publisher;
        }

        public List<ColumnInfoImpl> columns() {
            return columns;
        }

        public PageStreamPublisher publisher() {
            return publisher;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("not serialized");
        }
    }
}
