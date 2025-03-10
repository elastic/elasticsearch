/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

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

    public static final long MIN_KEEP_ALIVE = TimeValue.timeValueMinutes(1).millis();
    public static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);
    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.fromOptions(true, true, true, false);

    private String[] indices;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    private QueryBuilder filter = null;
    private String timestampField = FIELD_TIMESTAMP;
    private String tiebreakerField = null;
    private String eventCategoryField = FIELD_EVENT_CATEGORY;
    private int size = RequestDefaults.SIZE;
    private int fetchSize = RequestDefaults.FETCH_SIZE;
    private String query;
    private boolean ccsMinimizeRoundtrips = RequestDefaults.CCS_MINIMIZE_ROUNDTRIPS;
    private String resultPosition = "tail";
    private List<FieldAndFormat> fetchFields;
    private Map<String, Object> runtimeMappings = emptyMap();
    private int maxSamplesPerKey = RequestDefaults.MAX_SAMPLES_PER_KEY;
    private Boolean allowPartialSearchResults;
    private Boolean allowPartialSequenceResults;

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
    static final String KEY_MAX_SAMPLES_PER_KEY = "max_samples_per_key";
    static final String KEY_ALLOW_PARTIAL_SEARCH_RESULTS = "allow_partial_search_results";
    static final String KEY_ALLOW_PARTIAL_SEQUENCE_RESULTS = "allow_partial_sequence_results";

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
    static final ParseField MAX_SAMPLES_PER_KEY = new ParseField(KEY_MAX_SAMPLES_PER_KEY);
    static final ParseField ALLOW_PARTIAL_SEARCH_RESULTS = new ParseField(KEY_ALLOW_PARTIAL_SEARCH_RESULTS);
    static final ParseField ALLOW_PARTIAL_SEQUENCE_RESULTS = new ParseField(KEY_ALLOW_PARTIAL_SEQUENCE_RESULTS);

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
        this.ccsMinimizeRoundtrips = in.readBoolean();
        this.waitForCompletionTimeout = in.readOptionalTimeValue();
        this.keepAlive = in.readOptionalTimeValue();
        this.keepOnCompletion = in.readBoolean();
        resultPosition = in.readString();
        if (in.readBoolean()) {
            fetchFields = in.readCollectionAsList(FieldAndFormat::new);
        }
        runtimeMappings = in.readGenericMap();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            maxSamplesPerKey = in.readInt();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.EQL_ALLOW_PARTIAL_SEARCH_RESULTS)) {
            allowPartialSearchResults = in.readOptionalBoolean();
            allowPartialSequenceResults = in.readOptionalBoolean();
        } else {
            allowPartialSearchResults = false;
            allowPartialSequenceResults = false;
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

        if (keepAlive != null && keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException = addValidationError(
                "[keep_alive] must be greater than 1 minute, got:" + keepAlive.toString(),
                validationException
            );
        }

        if (runtimeMappings != null) {
            validationException = validateRuntimeMappings(runtimeMappings, validationException);
        }

        if (maxSamplesPerKey < 1) {
            validationException = addValidationError("max_samples_per_key must be greater than 0", validationException);
        }

        return validationException;
    }

    private static ActionRequestValidationException validateRuntimeMappings(
        Map<String, Object> runtimeMappings,
        ActionRequestValidationException validationException
    ) {
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
                return addValidationError(
                    "Expected map for runtime field [" + fieldName + "] definition but got [" + fieldName.getClass().getSimpleName() + "]",
                    validationException
                );
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
            builder.field(KEY_WAIT_FOR_COMPLETION_TIMEOUT, waitForCompletionTimeout.getStringRep());
        }
        if (keepAlive != null) {
            builder.field(KEY_KEEP_ALIVE, keepAlive.getStringRep());
        }
        builder.field(KEY_KEEP_ON_COMPLETION, keepOnCompletion);
        builder.field(KEY_RESULT_POSITION, resultPosition);
        if (fetchFields != null && fetchFields.isEmpty() == false) {
            builder.field(KEY_FETCH_FIELDS, fetchFields);
        }
        if (runtimeMappings != null) {
            builder.field(KEY_RUNTIME_MAPPINGS, runtimeMappings);
        }
        builder.field(KEY_MAX_SAMPLES_PER_KEY, maxSamplesPerKey);
        builder.field(KEY_ALLOW_PARTIAL_SEARCH_RESULTS, allowPartialSearchResults);
        builder.field(KEY_ALLOW_PARTIAL_SEQUENCE_RESULTS, allowPartialSequenceResults);

        return builder;
    }

    public static EqlSearchRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    protected static <R extends EqlSearchRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        ObjectParser<R, Void> parser = new ObjectParser<>("eql/search", false, supplier);
        parser.declareObject(EqlSearchRequest::filter, (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p), FILTER);
        parser.declareString(EqlSearchRequest::timestampField, TIMESTAMP_FIELD);
        parser.declareString(EqlSearchRequest::tiebreakerField, TIEBREAKER_FIELD);
        parser.declareString(EqlSearchRequest::eventCategoryField, EVENT_CATEGORY_FIELD);
        parser.declareInt(EqlSearchRequest::size, SIZE);
        parser.declareInt(EqlSearchRequest::fetchSize, FETCH_SIZE);
        parser.declareString(EqlSearchRequest::query, QUERY);
        parser.declareField(
            EqlSearchRequest::waitForCompletionTimeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEY_WAIT_FOR_COMPLETION_TIMEOUT),
            WAIT_FOR_COMPLETION_TIMEOUT,
            ObjectParser.ValueType.VALUE
        );
        parser.declareField(
            EqlSearchRequest::keepAlive,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEY_KEEP_ALIVE),
            KEEP_ALIVE,
            ObjectParser.ValueType.VALUE
        );
        parser.declareBoolean(EqlSearchRequest::keepOnCompletion, KEEP_ON_COMPLETION);
        parser.declareString(EqlSearchRequest::resultPosition, RESULT_POSITION);
        parser.declareField(EqlSearchRequest::fetchFields, EqlSearchRequest::parseFetchFields, FETCH_FIELDS_FIELD, ValueType.VALUE_ARRAY);
        parser.declareObject(EqlSearchRequest::runtimeMappings, (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
        parser.declareInt(EqlSearchRequest::maxSamplesPerKey, MAX_SAMPLES_PER_KEY);
        parser.declareBoolean(EqlSearchRequest::allowPartialSearchResults, ALLOW_PARTIAL_SEARCH_RESULTS);
        parser.declareBoolean(EqlSearchRequest::allowPartialSequenceResults, ALLOW_PARTIAL_SEQUENCE_RESULTS);
        return parser;
    }

    @Override
    public EqlSearchRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public QueryBuilder filter() {
        return this.filter;
    }

    public EqlSearchRequest filter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    public Map<String, Object> runtimeMappings() {
        return this.runtimeMappings;
    }

    public EqlSearchRequest runtimeMappings(Map<String, Object> runtimeMappings) {
        this.runtimeMappings = runtimeMappings;
        return this;
    }

    public String timestampField() {
        return this.timestampField;
    }

    public EqlSearchRequest timestampField(String timestampField) {
        this.timestampField = timestampField;
        return this;
    }

    public String tiebreakerField() {
        return this.tiebreakerField;
    }

    public EqlSearchRequest tiebreakerField(String tiebreakerField) {
        this.tiebreakerField = tiebreakerField;
        return this;
    }

    public String eventCategoryField() {
        return this.eventCategoryField;
    }

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

    public String query() {
        return this.query;
    }

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

    public int maxSamplesPerKey() {
        return maxSamplesPerKey;
    }

    public EqlSearchRequest maxSamplesPerKey(int maxSamplesPerKey) {
        this.maxSamplesPerKey = maxSamplesPerKey;
        return this;
    }

    public Boolean allowPartialSearchResults() {
        return allowPartialSearchResults;
    }

    public EqlSearchRequest allowPartialSearchResults(Boolean val) {
        this.allowPartialSearchResults = val;
        return this;
    }

    public Boolean allowPartialSequenceResults() {
        return allowPartialSequenceResults;
    }

    public EqlSearchRequest allowPartialSequenceResults(Boolean val) {
        this.allowPartialSequenceResults = val;
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
        out.writeBoolean(ccsMinimizeRoundtrips);
        out.writeOptionalTimeValue(waitForCompletionTimeout);
        out.writeOptionalTimeValue(keepAlive);
        out.writeBoolean(keepOnCompletion);
        out.writeString(resultPosition);
        out.writeBoolean(fetchFields != null);
        if (fetchFields != null) {
            out.writeCollection(fetchFields);
        }
        out.writeGenericMap(runtimeMappings);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            out.writeInt(maxSamplesPerKey);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.EQL_ALLOW_PARTIAL_SEARCH_RESULTS)) {
            out.writeOptionalBoolean(allowPartialSearchResults);
            out.writeOptionalBoolean(allowPartialSequenceResults);
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
        return size == that.size
            && fetchSize == that.fetchSize
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Objects.equals(filter, that.filter)
            && Objects.equals(timestampField, that.timestampField)
            && Objects.equals(tiebreakerField, that.tiebreakerField)
            && Objects.equals(eventCategoryField, that.eventCategoryField)
            && Objects.equals(query, that.query)
            && Objects.equals(ccsMinimizeRoundtrips, that.ccsMinimizeRoundtrips)
            && Objects.equals(waitForCompletionTimeout, that.waitForCompletionTimeout)
            && Objects.equals(keepAlive, that.keepAlive)
            && Objects.equals(resultPosition, that.resultPosition)
            && Objects.equals(fetchFields, that.fetchFields)
            && Objects.equals(runtimeMappings, that.runtimeMappings)
            && Objects.equals(maxSamplesPerKey, that.maxSamplesPerKey)
            && Objects.equals(allowPartialSearchResults, that.allowPartialSearchResults)
            && Objects.equals(allowPartialSequenceResults, that.allowPartialSequenceResults);
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
            ccsMinimizeRoundtrips,
            waitForCompletionTimeout,
            keepAlive,
            resultPosition,
            fetchFields,
            runtimeMappings,
            maxSamplesPerKey,
            allowPartialSearchResults,
            allowPartialSequenceResults
        );
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
