/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EqlSearchRequest extends ActionRequest implements IndicesRequest.Replaceable, ToXContent {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false,
        false, true, false);

    private QueryBuilder query = null;
    private String timestampField = "@timestamp";
    private String eventTypeField = "event.category";
    private String implicitJoinKeyField = "agent.id";
    private int fetchSize = 50;
    private SearchAfterBuilder searchAfterBuilder;
    private String rule;

    static final String KEY_QUERY = "query";
    static final String KEY_TIMESTAMP_FIELD = "timestamp_field";
    static final String KEY_EVENT_TYPE_FIELD = "event_type_field";
    static final String KEY_IMPLICIT_JOIN_KEY_FIELD = "implicit_join_key_field";
    static final String KEY_SIZE = "size";
    static final String KEY_SEARCH_AFTER = "search_after";
    static final String KEY_RULE = "rule";

    static final ParseField QUERY = new ParseField(KEY_QUERY);
    static final ParseField TIMESTAMP_FIELD = new ParseField(KEY_TIMESTAMP_FIELD);
    static final ParseField EVENT_TYPE_FIELD = new ParseField(KEY_EVENT_TYPE_FIELD);
    static final ParseField IMPLICIT_JOIN_KEY_FIELD = new ParseField(KEY_IMPLICIT_JOIN_KEY_FIELD);
    static final ParseField SIZE = new ParseField(KEY_SIZE);
    static final ParseField SEARCH_AFTER = new ParseField(KEY_SEARCH_AFTER);
    static final ParseField RULE = new ParseField(KEY_RULE);

    private static final ObjectParser<EqlSearchRequest, Void> PARSER = objectParser(EqlSearchRequest::new);

    public EqlSearchRequest() {
        super();
    }

    public EqlSearchRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        query = in.readOptionalNamedWriteable(QueryBuilder.class);
        timestampField = in.readString();
        eventTypeField = in.readString();
        implicitJoinKeyField = in.readString();
        fetchSize = in.readVInt();
        searchAfterBuilder = in.readOptionalWriteable(SearchAfterBuilder::new);
        rule = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        } else {
            for (String index : indices) {
                if (index == null) {
                    validationException = addValidationError("index is null", validationException);
                    break;
                }
            }
        }

        if (rule == null || rule.isEmpty()) {
            validationException = addValidationError("rule is null or empty", validationException);
        }

        if (timestampField == null || timestampField.isEmpty()) {
            validationException = addValidationError("timestamp field is null or empty", validationException);
        }

        if (eventTypeField == null || eventTypeField.isEmpty()) {
            validationException = addValidationError("event type field is null or empty", validationException);
        }

        if (implicitJoinKeyField == null || implicitJoinKeyField.isEmpty()) {
            validationException = addValidationError("implicit join key field is null or empty", validationException);
        }

        if (fetchSize <= 0) {
            validationException = addValidationError("size must be more than 0", validationException);
        }

        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (query != null) {
            builder.field(KEY_QUERY, query);
        }
        builder.field(KEY_TIMESTAMP_FIELD, timestampField());
        builder.field(KEY_EVENT_TYPE_FIELD, eventTypeField());
        if (implicitJoinKeyField != null) {
            builder.field(KEY_IMPLICIT_JOIN_KEY_FIELD, implicitJoinKeyField());
        }
        builder.field(KEY_SIZE, fetchSize());

        if (searchAfterBuilder != null) {
            builder.array(SEARCH_AFTER.getPreferredName(), searchAfterBuilder.getSortValues());
        }

        builder.field(KEY_RULE, rule);

        return builder;
    }

    public static EqlSearchRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    protected static <R extends EqlSearchRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        ObjectParser<R, Void> parser = new ObjectParser<>("eql/search", false, supplier);
        parser.declareObject(EqlSearchRequest::query,
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), QUERY);
        parser.declareString(EqlSearchRequest::timestampField, TIMESTAMP_FIELD);
        parser.declareString(EqlSearchRequest::eventTypeField, EVENT_TYPE_FIELD);
        parser.declareString(EqlSearchRequest::implicitJoinKeyField, IMPLICIT_JOIN_KEY_FIELD);
        parser.declareInt(EqlSearchRequest::fetchSize, SIZE);
        parser.declareField(EqlSearchRequest::setSearchAfter, SearchAfterBuilder::fromXContent, SEARCH_AFTER,
            ObjectParser.ValueType.OBJECT_ARRAY);
        parser.declareString(EqlSearchRequest::rule, RULE);
        return parser;
    }

    @Override
    public EqlSearchRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public QueryBuilder query() { return this.query; }

    public EqlSearchRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    public String timestampField() { return this.timestampField; }

    public EqlSearchRequest timestampField(String timestampField) {
        this.timestampField = timestampField;
        return this;
    }

    public String eventTypeField() { return this.eventTypeField; }

    public EqlSearchRequest eventTypeField(String eventTypeField) {
        this.eventTypeField = eventTypeField;
        return this;
    }

    public String implicitJoinKeyField() { return this.implicitJoinKeyField; }

    public EqlSearchRequest implicitJoinKeyField(String implicitJoinKeyField) {
        this.implicitJoinKeyField = implicitJoinKeyField;
        return this;
    }

    public int fetchSize() { return this.fetchSize; }

    public EqlSearchRequest fetchSize(int size) {
        this.fetchSize = size;
        return this;
    }

    public Object[] searchAfter() {
        if (searchAfterBuilder == null) {
            return null;
        }
        return searchAfterBuilder.getSortValues();
    }

    public EqlSearchRequest searchAfter(Object[] values) {
        this.searchAfterBuilder = new SearchAfterBuilder().setSortValues(values);
        return this;
    }

    private EqlSearchRequest setSearchAfter(SearchAfterBuilder builder) {
        this.searchAfterBuilder = builder;
        return this;
    }

    public String rule() { return this.rule; }

    public EqlSearchRequest rule(String rule) {
        this.rule = rule;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalNamedWriteable(query);
        out.writeString(timestampField);
        out.writeString(eventTypeField);
        out.writeString(implicitJoinKeyField);
        out.writeVInt(fetchSize);
        out.writeOptionalWriteable(searchAfterBuilder);
        out.writeString(rule);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EqlSearchRequest that = (EqlSearchRequest) o;
        return
            fetchSize == that.fetchSize &&
            Arrays.equals(indices, that.indices) &&
            Objects.equals(indicesOptions, that.indicesOptions) &&
            Objects.equals(query, that.query) &&
            Objects.equals(timestampField, that.timestampField) &&
            Objects.equals(eventTypeField, that.eventTypeField) &&
            Objects.equals(implicitJoinKeyField, that.implicitJoinKeyField) &&
            Objects.equals(searchAfterBuilder, that.searchAfterBuilder) &&
            Objects.equals(rule, that.rule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            Arrays.hashCode(indices),
            indicesOptions,
            query,
            fetchSize,
            timestampField,
            eventTypeField,
            implicitJoinKeyField,
            searchAfterBuilder,
            rule);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }
}
