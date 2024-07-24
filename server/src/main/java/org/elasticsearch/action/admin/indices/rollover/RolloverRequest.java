/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class to swap index under an alias or increment data stream generation upon satisfying conditions
 * <p>
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should also go to that client class.
 */
public class RolloverRequest extends AcknowledgedRequest<RolloverRequest> implements IndicesRequest {

    private static final ObjectParser<RolloverRequest, Boolean> PARSER = new ObjectParser<>("rollover");

    private static final ParseField CONDITIONS = new ParseField("conditions");

    static {
        PARSER.declareField(
            (parser, request, context) -> request.setConditions(RolloverConditions.fromXContent(parser)),
            CONDITIONS,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(
            (parser, request, context) -> request.createIndexRequest.settings(parser.map()),
            CreateIndexRequest.SETTINGS,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField((parser, request, includeTypeName) -> {
            if (includeTypeName) {
                // expecting one type only
                for (Map.Entry<String, Object> mappingsEntry : parser.map().entrySet()) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> value = (Map<String, Object>) mappingsEntry.getValue();
                    request.createIndexRequest.mapping(value);
                }
            } else {
                // a type is not included, add a dummy _doc type
                Map<String, Object> mappings = parser.map();
                if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
                    throw new IllegalArgumentException(
                        "The mapping definition cannot be nested under a type "
                            + "["
                            + MapperService.SINGLE_MAPPING_NAME
                            + "] unless include_type_name is set to true."
                    );
                }
                request.createIndexRequest.mapping(mappings);
            }
        }, CreateIndexRequest.MAPPINGS.forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.V_7)), ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, context) -> {
            // a type is not included, add a dummy _doc type
            Map<String, Object> mappings = parser.map();
            if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {

                throw new IllegalArgumentException("The mapping definition cannot be nested under a type");
            }
            request.createIndexRequest.mapping(mappings);
        }, CreateIndexRequest.MAPPINGS.forRestApiVersion(RestApiVersion.onOrAfter(RestApiVersion.V_8)), ObjectParser.ValueType.OBJECT);

        PARSER.declareField(
            (parser, request, context) -> request.createIndexRequest.aliases(parser.map()),
            CreateIndexRequest.ALIASES,
            ObjectParser.ValueType.OBJECT
        );
    }

    private String rolloverTarget;
    private String newIndexName;
    private boolean dryRun;
    private boolean lazy;
    private RolloverConditions conditions = new RolloverConditions();
    // the index name "_na_" is never read back, what matters are settings, mappings and aliases
    private CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");
    private IndicesOptions indicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    public RolloverRequest(StreamInput in) throws IOException {
        super(in);
        rolloverTarget = in.readString();
        newIndexName = in.readOptionalString();
        dryRun = in.readBoolean();
        conditions = new RolloverConditions(in);
        createIndexRequest = new CreateIndexRequest(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            lazy = in.readBoolean();
        } else {
            lazy = false;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            indicesOptions = IndicesOptions.readIndicesOptions(in);
        }
    }

    RolloverRequest() {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
    }

    public RolloverRequest(String rolloverTarget, String newIndexName) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        this.rolloverTarget = rolloverTarget;
        this.newIndexName = newIndexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = createIndexRequest.validate();
        if (rolloverTarget == null) {
            validationException = addValidationError("rollover target is missing", validationException);
        }

        // if the request has any conditions, then at least one condition must be a max_* condition
        if (conditions.hasMinConditions() && conditions.hasMaxConditions() == false) {
            validationException = addValidationError(
                "at least one max_* rollover condition must be set when using min_* conditions",
                validationException
            );
        }

        var failureStoreOptions = indicesOptions.failureStoreOptions();
        if (failureStoreOptions.includeRegularIndices() && failureStoreOptions.includeFailureIndices()) {
            validationException = addValidationError(
                "rollover cannot be applied to both regular and failure indices at the same time",
                validationException
            );
        }

        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(rolloverTarget);
        out.writeOptionalString(newIndexName);
        out.writeBoolean(dryRun);
        conditions.writeTo(out);
        createIndexRequest.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
            out.writeBoolean(lazy);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            indicesOptions.writeIndicesOptions(out);
        }
    }

    @Override
    public String[] indices() {
        return new String[] { rolloverTarget };
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * @return true of the rollover request targets the failure store, false otherwise.
     */
    public boolean targetsFailureStore() {
        return DataStream.isFailureStoreFeatureFlagEnabled() && indicesOptions.failureStoreOptions().includeFailureIndices();
    }

    public void setIndicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Sets the rollover target to roll over to another index
     */
    public void setRolloverTarget(String rolloverTarget) {
        this.rolloverTarget = rolloverTarget;
    }

    /**
     * Sets the alias to roll over to another index
     */
    public void setNewIndexName(String newIndexName) {
        this.newIndexName = newIndexName;
    }

    /**
     * Sets if the rollover should not be executed when conditions are met
     */
    public void dryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    /**
     * Sets the wait for active shards configuration for the rolled index that gets created.
     */
    public void setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        createIndexRequest.waitForActiveShards(waitForActiveShards);
    }

    /**
     * Sets the conditions that need to be met for the index to roll over
     */
    public void setConditions(RolloverConditions conditions) {
        this.conditions = conditions;
    }

    /**
     * Sets if an unconditional rollover should wait for a document to come before it gets executed
     */
    public void lazy(boolean lazy) {
        this.lazy = lazy;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public RolloverConditions getConditions() {
        return conditions;
    }

    public Collection<Condition<?>> getConditionValues() {
        return conditions.getConditions().values();
    }

    public String getRolloverTarget() {
        return rolloverTarget;
    }

    public String getNewIndexName() {
        return newIndexName;
    }

    public boolean isLazy() {
        return lazy;
    }

    /**
     * Given the results of evaluating each individual condition, determine whether the rollover request should proceed -- that is,
     * whether the conditions are met.
     *
     * If there are no conditions at all, then the request is unconditional (i.e. a command), and the conditions are met.
     *
     * If the request has conditions, then all min_* conditions and at least one max_* condition must have a true result.
     *
     * @param conditionResults a map of individual conditions and their associated evaluation results
     *
     * @return where the conditions for rollover are satisfied or not
     */
    public boolean areConditionsMet(Map<String, Boolean> conditionResults) {
        return conditions.areConditionsMet(conditionResults);
    }

    /**
     * Returns the inner {@link CreateIndexRequest}. Allows to configure mappings, settings and aliases for the new index.
     */
    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    // param isTypeIncluded decides how mappings should be parsed from XContent
    public void fromXContent(boolean isTypeIncluded, XContentParser parser) throws IOException {
        PARSER.parse(parser, this, isTypeIncluded);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RolloverRequest that = (RolloverRequest) o;
        return dryRun == that.dryRun
            && lazy == that.lazy
            && Objects.equals(rolloverTarget, that.rolloverTarget)
            && Objects.equals(newIndexName, that.newIndexName)
            && Objects.equals(conditions, that.conditions)
            && Objects.equals(createIndexRequest, that.createIndexRequest)
            && Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rolloverTarget, newIndexName, dryRun, conditions, createIndexRequest, lazy, indicesOptions);
    }
}
