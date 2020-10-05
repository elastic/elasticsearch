/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class to swap index under an alias or increment data stream generation upon satisfying conditions
 *
 * Note: there is a new class with the same name for the Java HLRC that uses a typeless format.
 * Any changes done to this class should also go to that client class.
 */
public class RolloverRequest extends AcknowledgedRequest<RolloverRequest> implements IndicesRequest {

    private static final ObjectParser<RolloverRequest, Void> PARSER = new ObjectParser<>("rollover");
    private static final ObjectParser<Map<String, Condition<?>>, Void> CONDITION_PARSER = new ObjectParser<>("conditions");

    private static final ParseField CONDITIONS = new ParseField("conditions");
    private static final ParseField MAX_AGE_CONDITION = new ParseField(MaxAgeCondition.NAME);
    private static final ParseField MAX_DOCS_CONDITION = new ParseField(MaxDocsCondition.NAME);
    private static final ParseField MAX_SIZE_CONDITION = new ParseField(MaxSizeCondition.NAME);

    static {
        CONDITION_PARSER.declareString((conditions, s) ->
                conditions.put(MaxAgeCondition.NAME, new MaxAgeCondition(TimeValue.parseTimeValue(s, MaxAgeCondition.NAME))),
                MAX_AGE_CONDITION);
        CONDITION_PARSER.declareLong((conditions, value) ->
                conditions.put(MaxDocsCondition.NAME, new MaxDocsCondition(value)), MAX_DOCS_CONDITION);
        CONDITION_PARSER.declareString((conditions, s) ->
                conditions.put(MaxSizeCondition.NAME, new MaxSizeCondition(ByteSizeValue.parseBytesSizeValue(s, MaxSizeCondition.NAME))),
                MAX_SIZE_CONDITION);

        PARSER.declareField((parser, request, context) -> CONDITION_PARSER.parse(parser, request.conditions, null),
            CONDITIONS, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, context) -> request.createIndexRequest.settings(parser.map()),
            CreateIndexRequest.SETTINGS, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, context) -> {
            // a type is not included, add a dummy _doc type
            Map<String, Object> mappings = parser.map();
            if (MapperService.isMappingSourceTyped(MapperService.SINGLE_MAPPING_NAME, mappings)) {
                throw new IllegalArgumentException("The mapping definition cannot be nested under a type");
            }
            request.createIndexRequest.mapping(mappings);
        }, CreateIndexRequest.MAPPINGS, ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, context) -> request.createIndexRequest.aliases(parser.map()),
            CreateIndexRequest.ALIASES, ObjectParser.ValueType.OBJECT);
    }

    private String rolloverTarget;
    private String newIndexName;
    private boolean dryRun;
    private final Map<String, Condition<?>> conditions = new HashMap<>(2);
    //the index name "_na_" is never read back, what matters are settings, mappings and aliases
    private CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

    public RolloverRequest(StreamInput in) throws IOException {
        super(in);
        rolloverTarget = in.readString();
        newIndexName = in.readOptionalString();
        dryRun = in.readBoolean();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            Condition<?> condition = in.readNamedWriteable(Condition.class);
            this.conditions.put(condition.name, condition);
        }
        createIndexRequest = new CreateIndexRequest(in);
    }

    RolloverRequest() {}

    public RolloverRequest(String rolloverTarget, String newIndexName) {
        this.rolloverTarget = rolloverTarget;
        this.newIndexName = newIndexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = createIndexRequest.validate();
        if (rolloverTarget == null) {
            validationException = addValidationError("rollover target is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(rolloverTarget);
        out.writeOptionalString(newIndexName);
        out.writeBoolean(dryRun);
        out.writeCollection(conditions.values(), StreamOutput::writeNamedWriteable);
        createIndexRequest.writeTo(out);
    }

    @Override
    public String[] indices() {
        return new String[] {rolloverTarget};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    /**
     * Sets the rollover target to rollover to another index
     */
    public void setRolloverTarget(String rolloverTarget) {
        this.rolloverTarget = rolloverTarget;
    }

    /**
     * Sets the alias to rollover to another index
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
     * Adds condition to check if the index is at least <code>age</code> old
     */
    public void addMaxIndexAgeCondition(TimeValue age) {
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(age);
        if (this.conditions.containsKey(maxAgeCondition.name)) {
            throw new IllegalArgumentException(maxAgeCondition.name + " condition is already set");
        }
        this.conditions.put(maxAgeCondition.name, maxAgeCondition);
    }

    /**
     * Adds condition to check if the index has at least <code>numDocs</code>
     */
    public void addMaxIndexDocsCondition(long numDocs) {
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(numDocs);
        if (this.conditions.containsKey(maxDocsCondition.name)) {
            throw new IllegalArgumentException(maxDocsCondition.name + " condition is already set");
        }
        this.conditions.put(maxDocsCondition.name, maxDocsCondition);
    }

    /**
     * Adds a size-based condition to check if the index size is at least <code>size</code>.
     */
    public void addMaxIndexSizeCondition(ByteSizeValue size) {
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(size);
        if (this.conditions.containsKey(maxSizeCondition.name)) {
            throw new IllegalArgumentException(maxSizeCondition + " condition is already set");
        }
        this.conditions.put(maxSizeCondition.name, maxSizeCondition);
    }


    public boolean isDryRun() {
        return dryRun;
    }

    public Map<String, Condition<?>> getConditions() {
        return conditions;
    }

    public String getRolloverTarget() {
        return rolloverTarget;
    }

    public String getNewIndexName() {
        return newIndexName;
    }

    /**
     * Returns the inner {@link CreateIndexRequest}. Allows to configure mappings, settings and aliases for the new index.
     */
    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    // param isTypeIncluded decides how mappings should be parsed from XContent
    public void fromXContent(XContentParser parser) throws IOException {
        PARSER.parse(parser, this, null);
    }
}
