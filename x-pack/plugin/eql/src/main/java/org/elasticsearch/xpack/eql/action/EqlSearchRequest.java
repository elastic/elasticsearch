/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class EqlSearchRequest extends ActionRequest implements CompositeIndicesRequest, ToXContent {

    private String index = "";
    private QueryBuilder query = null;
    private String timestampField = "@timestamp";
    private String eventTypeField = "event.category";
    private String implicitJoinKeyField = "agent.id";
    private int fetchSize = 50;
    private List<String> searchAfter = Collections.emptyList();
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
        index = in.readString();
        query = in.readOptionalNamedWriteable(QueryBuilder.class);
        timestampField = in.readString();
        eventTypeField = in.readString();
        implicitJoinKeyField = in.readString();
        fetchSize = in.readVInt();
        searchAfter = in.readList(StreamInput::readString);
        rule = in.readString();
    }

    public EqlSearchRequest(String index, QueryBuilder query,
                            String timestampField, String eventTypeField, String implicitJoinKeyField,
                            int fetchSize, List<String> searchAfter, String rule) {
        this.index = index;
        this.query = query;
        this.timestampField = timestampField;
        this.eventTypeField = eventTypeField;
        this.implicitJoinKeyField = implicitJoinKeyField;
        this.fetchSize = fetchSize;
        this.searchAfter = searchAfter;
        this.rule = rule;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
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

        if (this.searchAfter != null && !this.searchAfter.isEmpty()) {
            builder.startArray(KEY_SEARCH_AFTER);
            for (String val : this.searchAfter) {
                builder.value(val);
            }
            builder.endArray();
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
        parser.declareStringArray(EqlSearchRequest::searchAfter, SEARCH_AFTER);
        parser.declareString(EqlSearchRequest::rule, RULE);
        return parser;
    }

    public String index() { return this.index; }

    public EqlSearchRequest index(String index) {
        if (!Strings.isNullOrEmpty(index)) {
            this.index = index;
        }
        return this;
    }

    public QueryBuilder query() { return this.query; }

    public EqlSearchRequest query(QueryBuilder query) {
        this.query = query;
        return this;
    }

    public String timestampField() { return this.timestampField; }

    public EqlSearchRequest timestampField(String timestampField) {
        if (!Strings.isNullOrEmpty(timestampField)) {
            this.timestampField = timestampField;
        }
        return this;
    }

    public String eventTypeField() { return this.eventTypeField; }

    public EqlSearchRequest eventTypeField(String eventTypeField) {
        if (!Strings.isNullOrEmpty(eventTypeField)) {
            this.eventTypeField = eventTypeField;
        }
        return this;
    }

    public String implicitJoinKeyField() { return this.implicitJoinKeyField; }

    public EqlSearchRequest implicitJoinKeyField(String implicitJoinKeyField) {
        if (!Strings.isNullOrEmpty(eventTypeField)) {
            this.implicitJoinKeyField = implicitJoinKeyField;
        }
        return this;
    }

    public int fetchSize() { return this.fetchSize; }

    public EqlSearchRequest fetchSize(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be more than 0.");
        }
        this.fetchSize = size;
        return this;
    }

    public List<String> searchAfter() {
        return searchAfter;
    }

    public EqlSearchRequest searchAfter(List<String> searchAfter) {
        if (searchAfter != null && searchAfter.size() > 0) {
            this.searchAfter = searchAfter;
        }
        return this;
    }


    public String rule() { return this.rule; }

    public EqlSearchRequest rule(String rule) {
        // TODO: possibly attempt to parse the rule here
        this.rule = rule;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeOptionalNamedWriteable(query);
        out.writeString(timestampField);
        out.writeString(eventTypeField);
        out.writeString(implicitJoinKeyField);
        out.writeVInt(fetchSize);
        out.writeStringCollection(searchAfter);
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
            Objects.equals(index, that.index) &&
            Objects.equals(query, that.query) &&
            Objects.equals(timestampField, that.timestampField) &&
            Objects.equals(eventTypeField, that.eventTypeField) &&
            Objects.equals(implicitJoinKeyField, that.implicitJoinKeyField) &&
            Objects.equals(searchAfter, that.searchAfter) &&
            Objects.equals(rule, that.rule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, query, fetchSize, timestampField, eventTypeField, implicitJoinKeyField, searchAfter, rule);
    }
}
