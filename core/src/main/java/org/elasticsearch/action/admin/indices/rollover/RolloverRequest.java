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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class to swap index under an alias given some predicates
 * TODO: documentation
 */
public class RolloverRequest extends AcknowledgedRequest<RolloverRequest> implements IndicesRequest {

    private String sourceAlias;
    private boolean simulate;
    private Set<Condition> conditions = new HashSet<>(2);

    public static ObjectParser<Set<Condition>, ParseFieldMatcherSupplier> TLP_PARSER =
        new ObjectParser<>("conditions", null);
    static {
        TLP_PARSER.declareField((parser, conditions, parseFieldMatcherSupplier) ->
        Condition.PARSER.parse(parser, conditions,  () -> ParseFieldMatcher.EMPTY),
            new ParseField("conditions"), ObjectParser.ValueType.OBJECT);
    }

    RolloverRequest() {}

    public RolloverRequest(String sourceAlias) {
        this.sourceAlias = sourceAlias;
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
        simulate = in.readBoolean();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            this.conditions.add(in.readNamedWriteable(Condition.class));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceAlias);
        out.writeBoolean(simulate);
        out.writeVInt(conditions.size());
        for (Condition condition : conditions) {
            out.writeNamedWriteable(condition);
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

    public void simulate(boolean simulate) {
        this.simulate = simulate;
    }

    public void addMaxIndexAgeCondition(TimeValue age) {
        this.conditions.add(new MaxAgeCondition(age));
    }

    public void addMaxIndexDocsCondition(long docs) {
        this.conditions.add(new MaxDocsCondition(docs));
    }

    public boolean isSimulate() {
        return simulate;
    }

    public Set<Condition> getConditions() {
        return conditions;
    }

    public String getSourceAlias() {
        return sourceAlias;
    }

    public void source(BytesReference source) {
        XContentType xContentType = XContentFactory.xContentType(source);
        if (xContentType != null) {
            try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(source)) {
                TLP_PARSER.parse(parser, this.conditions, () -> ParseFieldMatcher.EMPTY);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse source for rollover index", e);
            }
        } else {
            throw new ElasticsearchParseException("failed to parse content type for rollover index source");
        }
    }

}
