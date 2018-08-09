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

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class MoveToStepRequest extends AcknowledgedRequest<MoveToStepRequest> implements ToXContentObject {
    static final ParseField CURRENT_KEY_FIELD = new ParseField("current_step");
    static final ParseField NEXT_KEY_FIELD = new ParseField("next_step");
    private static final ConstructingObjectParser<MoveToStepRequest, String> PARSER =
        new ConstructingObjectParser<>("move_to_step_request", false,
            (a, index) -> {
                StepKey currentStepKey = (StepKey) a[0];
                StepKey nextStepKey = (StepKey) a[1];
                return new MoveToStepRequest(index, currentStepKey, nextStepKey);
            });
    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> StepKey.parse(p), CURRENT_KEY_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, name) -> StepKey.parse(p), NEXT_KEY_FIELD);
    }

    private String index;
    private StepKey currentStepKey;
    private StepKey nextStepKey;

    public MoveToStepRequest(String index, StepKey currentStepKey, StepKey nextStepKey) {
        if (index == null) {
            throw new IllegalArgumentException("index cannot be null");
        }
        if (currentStepKey == null) {
            throw new IllegalArgumentException(CURRENT_KEY_FIELD.getPreferredName() + " cannot be null");
        }
        if (nextStepKey == null) {
            throw new IllegalArgumentException(NEXT_KEY_FIELD.getPreferredName() + " cannot be null");
        }
        this.index = index;
        this.currentStepKey = currentStepKey;
        this.nextStepKey = nextStepKey;
    }

    public MoveToStepRequest() {
    }

    public String getIndex() {
        return index;
    }

    public StepKey getCurrentStepKey() {
        return currentStepKey;
    }

    public StepKey getNextStepKey() {
        return nextStepKey;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public static MoveToStepRequest parseRequest(String name, XContentParser parser) {
        return PARSER.apply(parser, name);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.index = in.readString();
        this.currentStepKey = new StepKey(in);
        this.nextStepKey = new StepKey(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        currentStepKey.writeTo(out);
        nextStepKey.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, currentStepKey, nextStepKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        MoveToStepRequest other = (MoveToStepRequest) obj;
        return Objects.equals(index, other.index) && Objects.equals(currentStepKey, other.currentStepKey)
            && Objects.equals(nextStepKey, other.nextStepKey);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field(CURRENT_KEY_FIELD.getPreferredName(), currentStepKey)
            .field(NEXT_KEY_FIELD.getPreferredName(), nextStepKey)
            .endObject();
    }
}