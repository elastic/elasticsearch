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
     * Response for the streaming ES|QL query action. Carries the schema (available after analysis),
     * a publisher of result pages (produced incrementally as compute runs), and an optional
     * null-column mask computed via static analysis when {@code drop_null_columns} is requested.
     *
     * <p>{@code nullColumns} is {@code null} when the flag is off, or when no index-backed columns
     * exist in the output (no field-caps call needed). When non-null, {@code nullColumns[i] == true}
     * means column {@code i} (from {@link #columns()}) had no data in the queried indices and should
     * be omitted from the NDJSON output.
     */
    public static class Response extends ActionResponse {

        private final List<ColumnInfoImpl> columns;
        private final PageStreamPublisher publisher;
        private final boolean[] nullColumns;

        public Response(List<ColumnInfoImpl> columns, PageStreamPublisher publisher, boolean[] nullColumns) {
            this.columns = columns;
            this.publisher = publisher;
            this.nullColumns = nullColumns;
        }

        public List<ColumnInfoImpl> columns() {
            return columns;
        }

        public PageStreamPublisher publisher() {
            return publisher;
        }

        /**
         * Per-column null mask derived from static field-caps analysis, or {@code null} if
         * {@code drop_null_columns} was not requested. When non-null, {@code nullColumns[i] == true}
         * means column {@code i} should be omitted from the response.
         */
        public boolean[] nullColumns() {
            return nullColumns;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("not serialized");
        }
    }
}
