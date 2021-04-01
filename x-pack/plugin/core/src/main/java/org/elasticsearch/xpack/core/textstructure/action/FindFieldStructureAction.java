/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class FindFieldStructureAction extends ActionType<TextStructureResponse> {

    public static final FindFieldStructureAction INSTANCE = new FindFieldStructureAction();
    public static final String NAME = "cluster:monitor/text_structure/find_field_structure";

    private FindFieldStructureAction() {
        super(NAME, TextStructureResponse::new);
    }

    public static class Request extends AbstractFindStructureRequest {
        public static final ParseField INDEX = new ParseField("index");
        public static final ParseField FIELD_NAME = new ParseField("field_name");
        public static final ParseField QUERY = new ParseField("query");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(
            "find_field_structure_action",
            Request::new
        );

        static {
            PARSER.declareStringArray(Request::setIndices, INDEX);
            PARSER.declareString(Request::setFieldName, FIELD_NAME);
            PARSER.declareObject(Request::setQueryBuilder, (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), QUERY);
            PARSER.declareObject(Request::setRuntimeMappings, (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
        }

        public static Request fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        private String[] indices;
        private String fieldName;
        private QueryBuilder queryBuilder;
        private Map<String, Object> runtimeMappings = Collections.emptyMap();

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
            fieldName = in.readString();
            queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
            runtimeMappings = in.readMap();
        }

        public String[] getIndices() {
            return indices;
        }

        private void setIndices(List<String> indices) {
            setIndices(indices.toArray(String[]::new));
        }

        public Request setIndices(String[] indices) {
            this.indices = indices;
            return this;
        }

        public QueryBuilder getQueryBuilder() {
            return queryBuilder;
        }

        public Request setQueryBuilder(QueryBuilder queryBuilder) {
            this.queryBuilder = queryBuilder;
            return this;
        }

        public Map<String, Object> getRuntimeMappings() {
            return runtimeMappings;
        }

        public Request setRuntimeMappings(Map<String, Object> runtimeMappings) {
            this.runtimeMappings = runtimeMappings;
            return this;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Request setFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = super.validate();
            if (indices == null || indices.length == 0) {
                validationException = addValidationError("valid indices must be provided", validationException);
            }
            if (fieldName == null) {
                validationException = addValidationError("valid non-null field_name must be provided", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            out.writeString(fieldName);
            out.writeOptionalNamedWriteable(queryBuilder);
            out.writeMap(runtimeMappings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(linesToSample, lineMergeSizeLimit, timeout, charset, format, columnNames, hasHeaderRow, delimiter,
                grokPattern, timestampFormat, timestampField, Arrays.hashCode(indices), fieldName, queryBuilder, runtimeMappings);
        }

        @Override
        public boolean equals(Object other) {

            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            Request that = (Request) other;
            return Objects.equals(this.linesToSample, that.linesToSample) &&
                Objects.equals(this.lineMergeSizeLimit, that.lineMergeSizeLimit) &&
                Objects.equals(this.timeout, that.timeout) &&
                Objects.equals(this.charset, that.charset) &&
                Objects.equals(this.format, that.format) &&
                Objects.equals(this.columnNames, that.columnNames) &&
                Objects.equals(this.hasHeaderRow, that.hasHeaderRow) &&
                Objects.equals(this.delimiter, that.delimiter) &&
                Objects.equals(this.grokPattern, that.grokPattern) &&
                Objects.equals(this.timestampFormat, that.timestampFormat) &&
                Objects.equals(this.timestampField, that.timestampField) &&
                Arrays.equals(this.indices, that.indices) &&
                Objects.equals(this.fieldName, that.fieldName) &&
                Objects.equals(this.queryBuilder, that.queryBuilder) &&
                Objects.equals(this.runtimeMappings, that.runtimeMappings);
        }
    }
}
