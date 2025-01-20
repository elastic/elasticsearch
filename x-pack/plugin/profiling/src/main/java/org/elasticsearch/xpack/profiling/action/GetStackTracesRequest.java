/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.profiling.persistence.EventsIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;

/**
 * A request to get profiling details
 */
public class GetStackTracesRequest extends ActionRequest implements IndicesRequest.Replaceable {
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField SAMPLE_SIZE_FIELD = new ParseField("sample_size");
    public static final ParseField LIMIT_FIELD = new ParseField("limit");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField STACKTRACE_IDS_FIELD = new ParseField("stacktrace_ids_field");
    public static final ParseField AGGREGATION_FIELDS = new ParseField("aggregation_fields");
    public static final ParseField REQUESTED_DURATION_FIELD = new ParseField("requested_duration");
    public static final ParseField AWS_COST_FACTOR_FIELD = new ParseField("aws_cost_factor");
    public static final ParseField AZURE_COST_FACTOR_FIELD = new ParseField("azure_cost_factor");
    public static final ParseField CUSTOM_CO2_PER_KWH = new ParseField("co2_per_kwh");
    public static final ParseField CUSTOM_DATACENTER_PUE = new ParseField("datacenter_pue");
    public static final ParseField CUSTOM_PER_CORE_WATT_X86 = new ParseField("per_core_watt_x86");
    public static final ParseField CUSTOM_PER_CORE_WATT_ARM64 = new ParseField("per_core_watt_arm64");
    public static final ParseField CUSTOM_COST_PER_CORE_HOUR = new ParseField("cost_per_core_hour");
    private static final int DEFAULT_SAMPLE_SIZE = 20_000;

    private QueryBuilder query;
    private int sampleSize;
    private Integer limit;
    private String[] indices;
    private boolean userProvidedIndices;
    private String stackTraceIdsField;
    private String[] aggregationFields;
    private Double requestedDuration;
    private Double awsCostFactor;
    private Double azureCostFactor;
    private Double customCO2PerKWH;
    private Double customDatacenterPUE;
    private Double customPerCoreWattX86;
    private Double customPerCoreWattARM64;
    private Double customCostPerCoreHour;

    // We intentionally don't expose this field via the REST API, but we can control behavior within Elasticsearch.
    // Once we have migrated all client-side code to dedicated APIs (such as the flamegraph API), we can adjust
    // sample counts by default and remove this flag.
    private Boolean adjustSampleCount;

    // This is only meant for testing and is intentionally not exposed in the REST API.
    private Integer shardSeed;

    public GetStackTracesRequest() {
        this(null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public GetStackTracesRequest(
        Integer sampleSize,
        Double requestedDuration,
        Double awsCostFactor,
        Double azureCostFactor,
        QueryBuilder query,
        String[] indices,
        String stackTraceIdsField,
        String[] aggregationFields,
        Double customCO2PerKWH,
        Double customDatacenterPUE,
        Double customPerCoreWattX86,
        Double customPerCoreWattARM64,
        Double customCostPerCoreHour
    ) {
        this.sampleSize = sampleSize != null ? sampleSize : DEFAULT_SAMPLE_SIZE;
        this.requestedDuration = requestedDuration;
        this.awsCostFactor = awsCostFactor;
        this.azureCostFactor = azureCostFactor;
        this.query = query;
        this.indices = indices;
        this.userProvidedIndices = indices != null && indices.length > 0;
        this.stackTraceIdsField = stackTraceIdsField;
        this.aggregationFields = aggregationFields;
        this.customCO2PerKWH = customCO2PerKWH;
        this.customDatacenterPUE = customDatacenterPUE;
        this.customPerCoreWattX86 = customPerCoreWattX86;
        this.customPerCoreWattARM64 = customPerCoreWattARM64;
        this.customCostPerCoreHour = customCostPerCoreHour;
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public Integer getLimit() {
        return limit;
    }

    public Double getRequestedDuration() {
        return requestedDuration;
    }

    public Double getAwsCostFactor() {
        return awsCostFactor;
    }

    public Double getAzureCostFactor() {
        return azureCostFactor;
    }

    public Double getCustomCO2PerKWH() {
        return customCO2PerKWH;
    }

    public Double getCustomDatacenterPUE() {
        return customDatacenterPUE;
    }

    public Double getCustomPerCoreWattX86() {
        return customPerCoreWattX86;
    }

    public Double getCustomPerCoreWattARM64() {
        return customPerCoreWattARM64;
    }

    public Double getCustomCostPerCoreHour() {
        return customCostPerCoreHour;
    }

    public QueryBuilder getQuery() {
        return query;
    }

    public String[] getIndices() {
        return indices;
    }

    public boolean isUserProvidedIndices() {
        return userProvidedIndices;
    }

    public String getStackTraceIdsField() {
        return stackTraceIdsField;
    }

    public String[] getAggregationFields() {
        return aggregationFields;
    }

    public boolean hasAggregationFields() {
        String[] f = getAggregationFields();
        return f != null && f.length > 0;
    }

    public boolean isAdjustSampleCount() {
        return Boolean.TRUE.equals(adjustSampleCount);
    }

    public void setAdjustSampleCount(Boolean adjustSampleCount) {
        this.adjustSampleCount = adjustSampleCount;
    }

    public Integer getShardSeed() {
        return shardSeed;
    }

    public void setShardSeed(Integer shardSeed) {
        this.shardSeed = shardSeed;
    }

    public void parseXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT && (token = parser.nextToken()) != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + token + "]."
            );
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SAMPLE_SIZE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.sampleSize = parser.intValue();
                } else if (LIMIT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.limit = parser.intValue();
                } else if (STACKTRACE_IDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.stackTraceIdsField = parser.text();
                } else if (REQUESTED_DURATION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.requestedDuration = parser.doubleValue();
                } else if (AWS_COST_FACTOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.awsCostFactor = parser.doubleValue();
                } else if (AZURE_COST_FACTOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.azureCostFactor = parser.doubleValue();
                } else if (CUSTOM_CO2_PER_KWH.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.customCO2PerKWH = parser.doubleValue();
                } else if (CUSTOM_DATACENTER_PUE.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.customDatacenterPUE = parser.doubleValue();
                } else if (CUSTOM_PER_CORE_WATT_X86.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.customPerCoreWattX86 = parser.doubleValue();
                } else if (CUSTOM_PER_CORE_WATT_ARM64.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.customPerCoreWattARM64 = parser.doubleValue();
                } else if (CUSTOM_COST_PER_CORE_HOUR.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.customCostPerCoreHour = parser.doubleValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.query = parseTopLevelQuery(parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.indices = parseToStringArray(parser, INDICES_FIELD);
                    this.userProvidedIndices = true;
                } else if (AGGREGATION_FIELDS.match(currentFieldName, parser.getDeprecationHandler())) {
                    this.aggregationFields = parseToStringArray(parser, AGGREGATION_FIELDS);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].");
            }
        }

        token = parser.nextToken();
        if (token != null) {
            throw new ParsingException(parser.getTokenLocation(), "Unexpected token [" + token + "] found after the main object.");
        }
    }

    private String[] parseToStringArray(XContentParser parser, ParseField parseField) throws IOException {
        XContentParser.Token token;
        List<String> values = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_STRING) {
                values.add(parser.text());
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Expected ["
                        + XContentParser.Token.VALUE_STRING
                        + "] but found ["
                        + token
                        + "] in ["
                        + parseField.getPreferredName()
                        + "]."
                );
            }
        }
        return values.toArray(new String[0]);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (userProvidedIndices) {
            if (stackTraceIdsField == null || stackTraceIdsField.isEmpty()) {
                validationException = addValidationError(
                    "[" + STACKTRACE_IDS_FIELD.getPreferredName() + "] is mandatory",
                    validationException
                );
            }
        } else {
            if (stackTraceIdsField != null) {
                validationException = addValidationError(
                    "[" + STACKTRACE_IDS_FIELD.getPreferredName() + "] must not be set",
                    validationException
                );
            }
        }
        if (aggregationFields != null) {
            // limit so we avoid an explosion of buckets
            if (aggregationFields.length < 1 || aggregationFields.length > 2) {
                validationException = addValidationError(
                    "["
                        + AGGREGATION_FIELDS.getPreferredName()
                        + "] must contain either one or two elements but contains ["
                        + aggregationFields.length
                        + "] elements.",
                    validationException
                );
            }

        }

        validationException = requirePositive(SAMPLE_SIZE_FIELD, sampleSize, validationException);
        validationException = requirePositive(LIMIT_FIELD, limit, validationException);
        validationException = requirePositive(REQUESTED_DURATION_FIELD, requestedDuration, validationException);
        validationException = requirePositive(AWS_COST_FACTOR_FIELD, awsCostFactor, validationException);
        validationException = requirePositive(AZURE_COST_FACTOR_FIELD, azureCostFactor, validationException);
        validationException = requirePositive(CUSTOM_CO2_PER_KWH, customCO2PerKWH, validationException);
        validationException = requirePositive(CUSTOM_DATACENTER_PUE, customDatacenterPUE, validationException);
        validationException = requirePositive(CUSTOM_PER_CORE_WATT_X86, customPerCoreWattX86, validationException);
        validationException = requirePositive(CUSTOM_PER_CORE_WATT_ARM64, customPerCoreWattARM64, validationException);
        validationException = requirePositive(CUSTOM_COST_PER_CORE_HOUR, customCostPerCoreHour, validationException);
        return validationException;
    }

    private static ActionRequestValidationException requirePositive(ParseField field, Number value, ActionRequestValidationException e) {
        if (value != null) {
            if (value.doubleValue() <= 0.0d) {
                return addValidationError("[" + field.getPreferredName() + "] must be greater than 0, got: " + value, e);
            }
        }
        return e;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                // generating description lazily since the query could be large
                StringBuilder sb = new StringBuilder();
                appendField(sb, "indices", indices);
                appendField(sb, "stacktrace_ids_field", stackTraceIdsField);
                appendField(sb, "aggregation_fields", aggregationFields);
                appendField(sb, "sample_size", sampleSize);
                appendField(sb, "limit", limit);
                appendField(sb, "requested_duration", requestedDuration);
                appendField(sb, "aws_cost_factor", awsCostFactor);
                appendField(sb, "azure_cost_factor", azureCostFactor);
                appendField(sb, "co2_per_kwh", customCO2PerKWH);
                appendField(sb, "datacenter_pue", customDatacenterPUE);
                appendField(sb, "per_core_watt_x86", customPerCoreWattX86);
                appendField(sb, "per_core_watt_arm64", customPerCoreWattARM64);
                appendField(sb, "cost_per_core_hour", customCostPerCoreHour);
                appendField(sb, "query", query);
                return sb.toString();
            }
        };
    }

    private static void appendField(StringBuilder sb, String name, Object value) {
        if (sb.isEmpty() == false) {
            sb.append(", ");
        }
        if (value == null) {
            sb.append(name).append("[]");
        } else {
            sb.append(name).append("[").append(value).append("]");
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
        GetStackTracesRequest that = (GetStackTracesRequest) o;
        return Objects.equals(query, that.query)
            && Objects.equals(sampleSize, that.sampleSize)
            && Arrays.equals(indices, that.indices)
            && Objects.equals(stackTraceIdsField, that.stackTraceIdsField);
    }

    @Override
    public int hashCode() {
        // The object representation of `query` may use Lucene's ByteRef to represent values. This class' hashCode implementation
        // uses StringUtils.GOOD_FAST_HASH_SEED which is reinitialized for each JVM. This means that hashcode is consistent *within*
        // a JVM but will not be consistent across the cluster. As we use hashCode e.g. to initialize the random number generator in
        // Resampler to produce a consistent downsampling results, relying on the default hashCode implementation of `query` will
        // produce consistent results per node but not across the cluster. To avoid this, we produce the hashCode based on the
        // string representation instead, which will produce consistent results for the entire cluster and across node restarts.
        return Objects.hash(Objects.toString(query, "null"), sampleSize, Arrays.hashCode(indices), stackTraceIdsField);
    }

    @Override
    public String[] indices() {
        Set<String> indices = new HashSet<>();
        indices.add("profiling-stacktraces");
        indices.add("profiling-stackframes");
        indices.add("profiling-executables");
        if (userProvidedIndices) {
            indices.addAll(List.of(this.indices));
        } else {
            indices.addAll(EventsIndex.indexNames());
        }
        return indices.toArray(new String[0]);
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.STRICT_EXPAND_OPEN;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        validateIndices(indices);
        this.indices = indices;
        return null;
    }

    private static void validateIndices(String... indices) {
        Objects.requireNonNull(indices, "indices must not be null");
        for (String index : indices) {
            Objects.requireNonNull(index, "index must not be null");
        }
    }
}
