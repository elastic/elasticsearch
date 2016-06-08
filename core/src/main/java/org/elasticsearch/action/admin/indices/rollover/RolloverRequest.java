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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class to swap index under an alias given some predicates
 * TODO: documentation
 */
public class RolloverRequest extends AcknowledgedRequest<RolloverRequest> implements IndicesRequest {

    private String alias;
    private boolean simulate;
    private Set<Condition> conditions = new HashSet<>(2);
    private CreateIndexRequest createIndexRequest = new CreateIndexRequest("_na_");

    public static ObjectParser<RolloverRequest, ParseFieldMatcherSupplier> PARSER =
        new ObjectParser<>("conditions", null);
    static {
        PARSER.declareField((parser, request, parseFieldMatcherSupplier) ->
            Condition.PARSER.parse(parser, request.conditions, parseFieldMatcherSupplier),
            new ParseField("conditions"), ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, parseFieldMatcherSupplier) ->
            request.createIndexRequest.settings(parser.map()),
            new ParseField("settings"), ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, parseFieldMatcherSupplier) -> {
            for (Map.Entry<String, Object> mappingsEntry : parser.map().entrySet()) {
                request.createIndexRequest.mapping(mappingsEntry.getKey(),
                    (Map<String, Object>) mappingsEntry.getValue());
            }
        }, new ParseField("mappings"), ObjectParser.ValueType.OBJECT);
        PARSER.declareField((parser, request, parseFieldMatcherSupplier) ->
            request.createIndexRequest.aliases(parser.map()),
            new ParseField("aliases"), ObjectParser.ValueType.OBJECT);
    }

    RolloverRequest() {}

    public RolloverRequest(String alias) {
        this.alias = alias;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = createIndexRequest == null ? null : createIndexRequest.validate();
        if (alias == null) {
            validationException = addValidationError("index alias is missing", validationException);
        }
        if (createIndexRequest == null) {
            validationException = addValidationError("create index request is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        alias = in.readString();
        simulate = in.readBoolean();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            this.conditions.add(in.readNamedWriteable(Condition.class));
        }
        createIndexRequest = new CreateIndexRequest();
        createIndexRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(alias);
        out.writeBoolean(simulate);
        out.writeVInt(conditions.size());
        for (Condition condition : conditions) {
            out.writeNamedWriteable(condition);
        }
        createIndexRequest.writeTo(out);
    }

    @Override
    public String[] indices() {
        return new String[] {alias};
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    public void setAlias(String alias) {
        this.alias = alias;
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

    public String getAlias() {
        return alias;
    }

    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    public void setCreateIndexRequest(CreateIndexRequest createIndexRequest) {
        this.createIndexRequest = Objects.requireNonNull(createIndexRequest, "create index request must not be null");;
    }

    public void source(BytesReference source) {
        XContentType xContentType = XContentFactory.xContentType(source);
        if (xContentType != null) {
            try (XContentParser parser = XContentFactory.xContent(xContentType).createParser(source)) {
                PARSER.parse(parser, this, () -> ParseFieldMatcher.EMPTY);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parse source for rollover index", e);
            }
        } else {
            throw new ElasticsearchParseException("failed to parse content type for rollover index source");
        }
    }

}
