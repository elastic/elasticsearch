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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class to swap index under an alias given some predicates
 * TODO: documentation
 */
public class RolloverRequest extends AcknowledgedRequest<RolloverRequest> implements IndicesRequest {

    private String sourceAlias;
    private String optionalTargetAlias;
    private List<Condition> conditions = new ArrayList<>();

    RolloverRequest() {}

    public RolloverRequest(String sourceAlias, String optionalTargetAlias) {
        this.sourceAlias = sourceAlias;
        this.optionalTargetAlias = optionalTargetAlias;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceAlias == null) {
            validationException = addValidationError("source alias is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sourceAlias = in.readString();
        optionalTargetAlias = in.readOptionalString();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            Condition.ConditionType type = Condition.ConditionType.fromId(in.readByte());
            long value = in.readLong();
            this.conditions.add(new Condition(type, value));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceAlias);
        out.writeOptionalString(optionalTargetAlias);
        out.writeVInt(conditions.size());
        for (Condition condition : conditions) {
            out.writeByte(condition.getType().getId());
            out.writeLong(condition.getValue());
        }
    }

    @Override
    public String[] indices() {
        return new String[] {sourceAlias};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public void setSourceAlias(String sourceAlias) {
        this.sourceAlias = sourceAlias;
    }

    public void setOptionalTargetAlias(String optionalTargetAlias) {
        this.optionalTargetAlias = optionalTargetAlias;
    }

    public void addMaxIndexAgeCondition(String age) {
        addCondition(Condition.ConditionType.MAX_AGE, Condition.ConditionType.parseFromString(Condition.ConditionType.MAX_AGE, age));
    }

    public void addMaxIndexDocsCondition(String docs) {
        addCondition(Condition.ConditionType.MAX_DOCS, Condition.ConditionType.parseFromString(Condition.ConditionType.MAX_DOCS, docs));
    }

    public void addMaxIndexSizeCondition(String size) {
        addCondition(Condition.ConditionType.MAX_SIZE, Condition.ConditionType.parseFromString(Condition.ConditionType.MAX_SIZE, size));
    }

    private void addCondition(Condition.ConditionType conditionType, long value) {
        this.conditions.add(new Condition(conditionType, value));
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public String getSourceAlias() {
        return sourceAlias;
    }

    public String getOptionalTargetAlias() {
        return optionalTargetAlias;
    }

    public void source(BytesReference source) {
        XContentType xContentType = XContentFactory.xContentType(source);
        if (xContentType != null) {
            try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(source)) {
                source(parser.map());
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse source for rollover index", e);
            }
        } else {
            throw new ElasticsearchParseException("failed to parse content type for rollover index source");
        }
    }

    private void source(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().equals("conditions")) {
                final Map<String, String> conditions = (Map<String, String>) entry.getValue();
                for (Map.Entry<String, String> conditionEntry : conditions.entrySet()) {
                    final Condition.ConditionType conditionType = Condition.ConditionType.fromString(conditionEntry.getKey());
                    this.addCondition(conditionType, Condition.ConditionType.parseFromString(conditionType,
                        conditionEntry.getValue()));
                }
            } else {
                throw new ElasticsearchParseException("unknown property [" + entry.getKey() + "]");
            }
        }
    }
}
