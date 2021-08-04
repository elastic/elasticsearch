/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_EVENT_CATEGORY;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_TIMESTAMP;

public class EqlSearchRequest extends ActionRequest implements IndicesRequest.Replaceable, ToXContent {

    public static long MIN_KEEP_ALIVE = TimeValue.timeValueMinutes(1).millis();
    public static TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(true, true, true, false);

    private QueryBuilder filter = null;
    private String timestampField = FIELD_TIMESTAMP;
    private String tiebreakerField = null;
    private String eventCategoryField = FIELD_EVENT_CATEGORY;
    private int size = RequestDefaults.SIZE;
    private int fetchSize = RequestDefaults.FETCH_SIZE;
    private boolean ccsMinimizeRoundtrips = RequestDefaults.CCS_MINIMIZE_ROUNDTRIPS;
    private String query;
    private String resultPosition = "tail";
    private List<FieldAndFormat> fetchFields;
    private Map<String, Object> runtimeMappings = emptyMap();

    // Async settings
    private TimeValue waitForCompletionTimeout = null;
    private TimeValue keepAlive = DEFAULT_KEEP_ALIVE;
    private boolean keepOnCompletion;

    static final String KEY_FILTER = "filter";
    static final String KEY_TIMESTAMP_FIELD = "timestamp_field";
    static final String KEY_TIEBREAKER_FIELD = "tiebreaker_field";
    static final String KEY_EVENT_CATEGORY_FIELD = "event_category_field";
    static final String KEY_SIZE = "size";
    static final String KEY_FETCH_SIZE = "fetch_size";
    static final String KEY_QUERY = "query";
    static final String KEY_WAIT_FOR_COMPLETION_TIMEOUT = "wait_for_completion_timeout";
    static final String KEY_KEEP_ALIVE = "keep_alive";
    static final String KEY_KEEP_ON_COMPLETION = "keep_on_completion";
    static final String KEY_RESULT_POSITION = "result_position";
    static final String KEY_FETCH_FIELDS = "fields";
    static final String KEY_RUNTIME_MAPPINGS = "runtime_mappings";

    static final ParseField FILTER = new ParseField(KEY_FILTER);
    static final ParseField TIMESTAMP_FIELD = new ParseField(KEY_TIMESTAMP_FIELD);
    static final ParseField TIEBREAKER_FIELD = new ParseField(KEY_TIEBREAKER_FIELD);
    static final ParseField EVENT_CATEGORY_FIELD = new ParseField(KEY_EVENT_CATEGORY_FIELD);
    static final ParseField SIZE = new ParseField(KEY_SIZE);
    static final ParseField FETCH_SIZE = new ParseField(KEY_FETCH_SIZE);
    static final ParseField QUERY = new ParseField(KEY_QUERY);
    static final ParseField WAIT_FOR_COMPLETION_TIMEOUT = new ParseField(KEY_WAIT_FOR_COMPLETION_TIMEOUT);
    static final ParseField KEEP_ALIVE = new ParseField(KEY_KEEP_ALIVE);
    static final ParseField KEEP_ON_COMPLETION = new ParseField(KEY_KEEP_ON_COMPLETION);
    static final ParseField RESULT_POSITION = new ParseField(KEY_RESULT_POSITION);
    static final ParseField FETCH_FIELDS_FIELD = SearchSourceBuilder.FETCH_FIELDS_FIELD;

    private static final ObjectParser<EqlSearchRequest, Void> PARSER = objectParser(EqlSearchRequest::new);

    public EqlSearchRequest() {
        super();
    }

    public EqlSearchRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        timestampField = in.readString();
        tiebreakerField = in.readOptionalString();
        eventCategoryField = in.readString();
        size = in.readVInt();
        fetchSize = in.readVInt();
        query = in.readString();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO: Remove after backport
            this.waitForCompletionTimeout = in.readOptionalTimeValue();
            this.keepAlive = in.readOptionalTimeValue();
            this.keepOnCompletion = in.readBoolean();
        }
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            resultPosition = in.readString();
        }
        if (in.getVersion().onOrAfter(Version.V_7_13_0)) {
            if (in.readBoolean()) {
                fetchFields = in.readList(FieldAndFormat::new);
            }
            runtimeMappings = in.readMap();
        } else {
            runtimeMappings = emptyMap();
        }
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

        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is null", validationException);
        }

        if (query == null || query.isEmpty()) {
            validationException = addValidationError("query is null or empty", validationException);
        }

        if (timestampField == null || timestampField.isEmpty()) {
            validationException = addValidationError("@timestamp field is null or empty", validationException);
        }

        if (eventCategoryField == null || eventCategoryField.isEmpty()) {
            validationException = addValidationError("event category field is null or empty", validationException);
        }

        if (size < 0) {
            validationException = addValidationError("size must be greater than or equal to 0", validationException);
        }

        if (fetchSize < 2) {
            validationException = addValidationError("fetch size must be greater than 1", validationException);
        }

        if (keepAlive != null  && keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException =
                addValidationError("[keep_alive] must be greater than 1 minute, got:" + keepAlive.toString(), validationException);
        }

        if (runtimeMappings != null) {
            validationException = validateRuntimeMappings(runtimeMappings, validationException);
        }

        return validationException;
    }

    private static ActionRequestValidationException validateRuntimeMappings(Map<String, Object> runtimeMappings,
        ActionRequestValidationException validationException) {
        for (Map.Entry<String, Object> entry : runtimeMappings.entrySet()) {
            // top level objects are fields
            String fieldName = entry.getKey();
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                if (propNode.get("type") == null) {
                    return addValidationError("No type specified for runtime field [" + fieldName + "]", validationException);
                }
            } else {
                return addValidationError("Expected map for runtime field [" + fieldName + "] definition but got ["
                    + fieldName.getClass().getSimpleName() + "]", validationException);
            }
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (filter != null) {
            builder.field(KEY_FILTER, filter);
        }
        builder.field(KEY_TIMESTAMP_FIELD, timestampField());
        if (tiebreakerField != null) {
            builder.field(KEY_TIEBREAKER_FIELD, tiebreakerField());
        }
        builder.field(KEY_EVENT_CATEGORY_FIELD, eventCategoryField());
        builder.field(KEY_SIZE, size());
        builder.field(KEY_FETCH_SIZE, fetchSize());
        builder.field(KEY_QUERY, query);
        if (waitForCompletionTimeout != null) {
            builder.field(KEY_WAIT_FOR_COMPLETION_TIMEOUT, waitForCompletionTimeout);
        }
        if (keepAlive != null) {
            builder.field(KEY_KEEP_ALIVE, keepAlive);
        }
        builder.field(KEY_KEEP_ON_COMPLETION, keepOnCompletion);
        builder.field(KEY_RESULT_POSITION, resultPosition);
        if (fetchFields != null && fetchFields.isEmpty() == false) {
            builder.field(KEY_FETCH_FIELDS, fetchFields);
        }
        if (runtimeMappings != null) {
            builder.field(KEY_RUNTIME_MAPPINGS, runtimeMappings);
        }

        return builder;
    }

    public static EqlSearchRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    protected static <R extends EqlSearchRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        ObjectParser<R, Void> parser = new ObjectParser<>("eql/search", false, supplier);
        parser.declareObject(EqlSearchRequest::filter,
            (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), FILTER);
        parser.declareString(EqlSearchRequest::timestampField, TIMESTAMP_FIELD);
        parser.declareString(EqlSearchRequest::tiebreakerField, TIEBREAKER_FIELD);
        parser.declareString(EqlSearchRequest::eventCategoryField, EVENT_CATEGORY_FIELD);
        parser.declareInt(EqlSearchRequest::size, SIZE);
        parser.declareInt(EqlSearchRequest::fetchSize, FETCH_SIZE);
        parser.declareString(EqlSearchRequest::query, QUERY);
        parser.declareField(EqlSearchRequest::waitForCompletionTimeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEY_WAIT_FOR_COMPLETION_TIMEOUT), WAIT_FOR_COMPLETION_TIMEOUT,
            ObjectParser.ValueType.VALUE);
        parser.declareField(EqlSearchRequest::keepAlive,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEY_KEEP_ALIVE), KEEP_ALIVE, ObjectParser.ValueType.VALUE);
        parser.declareBoolean(EqlSearchRequest::keepOnCompletion, KEEP_ON_COMPLETION);
        parser.declareString(EqlSearchRequest::resultPosition, RESULT_POSITION);
        parser.declareField(EqlSearchRequest::fetchFields, EqlSearchRequest::parseFetchFields, FETCH_FIELDS_FIELD, ValueType.VALUE_ARRAY);
        parser.declareObject(EqlSearchRequest::runtimeMappings, (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
        return parser;
    }

    @Override
    public EqlSearchRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public QueryBuilder filter() { return this.filter; }

    public EqlSearchRequest filter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    public Map<String, Object> runtimeMappings() { return this.runtimeMappings; }

    public EqlSearchRequest runtimeMappings(Map<String, Object> runtimeMappings) {
        this.runtimeMappings = runtimeMappings;
        return this;
    }

    public String timestampField() { return this.timestampField; }

    public EqlSearchRequest timestampField(String timestampField) {
        this.timestampField = timestampField;
        return this;
    }

    public String tiebreakerField() { return this.tiebreakerField; }

    public EqlSearchRequest tiebreakerField(String tiebreakerField) {
        this.tiebreakerField = tiebreakerField;
        return this;
    }

    public String eventCategoryField() { return this.eventCategoryField; }

    public EqlSearchRequest eventCategoryField(String eventCategoryField) {
        this.eventCategoryField = eventCategoryField;
        return this;
    }

    public int size() {
        return this.size;
    }

    public EqlSearchRequest size(int size) {
        this.size = size;
        return this;
    }

    public int fetchSize() {
        return this.fetchSize;
    }

    public EqlSearchRequest fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public String query() { return this.query; }

    public EqlSearchRequest query(String query) {
        this.query = query;
        return this;
    }

    public TimeValue waitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public EqlSearchRequest waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
        return this;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    public EqlSearchRequest keepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public boolean keepOnCompletion() {
        return keepOnCompletion;
    }

    public EqlSearchRequest keepOnCompletion(boolean keepOnCompletion) {
        this.keepOnCompletion = keepOnCompletion;
        return this;
    }

    public EqlSearchRequest ccsMinimizeRoundtrips(boolean ccsMinimizeRoundtrips) {
        this.ccsMinimizeRoundtrips = ccsMinimizeRoundtrips;
        return this;
    }

    public boolean ccsMinimizeRoundtrips() {
        return ccsMinimizeRoundtrips;
    }

    public String resultPosition() {
        return resultPosition;
    }

    public EqlSearchRequest resultPosition(String position) {
        if ("head".equals(position) || "tail".equals(position)) {
            resultPosition = position;
        } else {
            throw new IllegalArgumentException("result position needs to be 'head' or 'tail', received '" + position + "'");
        }
        return this;
    }

    public List<FieldAndFormat> fetchFields() {
        return fetchFields;
    }

    public EqlSearchRequest fetchFields(List<FieldAndFormat> fetchFields) {
        this.fetchFields = fetchFields;
        return this;
    }

    private static List<FieldAndFormat> parseFetchFields(XContentParser parser) throws IOException {
        List<FieldAndFormat> result = new ArrayList<>();
        Token token = parser.currentToken();

        if (token == Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                result.add(FieldAndFormat.fromXContent(parser));
            }
        }
        return result.isEmpty() ? null : result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalNamedWriteable(filter);
        out.writeString(timestampField);
        out.writeOptionalString(tiebreakerField);
        out.writeString(eventCategoryField);
        out.writeVInt(size);
        out.writeVInt(fetchSize);
        out.writeString(query);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO: Remove after backport
            out.writeOptionalTimeValue(waitForCompletionTimeout);
            out.writeOptionalTimeValue(keepAlive);
            out.writeBoolean(keepOnCompletion);
        }

        if (out.getVersion().onOrAfter(Version.V_7_10_0)) { // TODO: Remove after backport
            out.writeString(resultPosition);
        }
        if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
            out.writeBoolean(fetchFields != null);
            if (fetchFields != null) {
                out.writeList(fetchFields);
            }
            out.writeMap(runtimeMappings);
        }
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
        return size == that.size &&
                fetchSize == that.fetchSize &&
                Arrays.equals(indices, that.indices) &&
                Objects.equals(indicesOptions, that.indicesOptions) &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(timestampField, that.timestampField) &&
                Objects.equals(tiebreakerField, that.tiebreakerField) &&
                Objects.equals(eventCategoryField, that.eventCategoryField) &&
                Objects.equals(query, that.query) &&
                Objects.equals(waitForCompletionTimeout, that.waitForCompletionTimeout) &&
                Objects.equals(keepAlive, that.keepAlive) &&
                Objects.equals(resultPosition, that.resultPosition) &&
                Objects.equals(fetchFields, that.fetchFields) &&
                Objects.equals(runtimeMappings, that.runtimeMappings);
    }


    @Override
    public int hashCode() {
        return Objects.hash(
            Arrays.hashCode(indices),
            indicesOptions,
            filter,
            size,
            fetchSize,
            timestampField,
            tiebreakerField,
            eventCategoryField,
            query,
            waitForCompletionTimeout,
            keepAlive,
            resultPosition,
            fetchFields,
            runtimeMappings);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    public EqlSearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new EqlSearchTask(id, type, action, getDescription(), parentTaskId, headers, null, null, keepAlive);
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("indices[");
        Strings.arrayToDelimitedString(indices, ",", sb);
        sb.append("], ");
        sb.append(query);
        return sb.toString();
    }
}
