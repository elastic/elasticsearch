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
package org.elasticsearch.action.support.master;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Abstract class that allows to mark action responses that support acknowledgements.
 * Facilitates consistency across different api.
 */
public abstract class AcknowledgedResponse extends ActionResponse {

    private static final String ACKNOWLEDGED = "acknowledged";
    private static final ParseField ACKNOWLEDGED_PARSER = new ParseField(ACKNOWLEDGED);

    protected static <T extends AcknowledgedResponse> void declareAcknowledgedField(ConstructingObjectParser<T, Void> PARSER) {
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ACKNOWLEDGED_PARSER,
            ObjectParser.ValueType.BOOLEAN);
    }

    private boolean acknowledged;

    protected AcknowledgedResponse() {

    }

    protected AcknowledgedResponse(boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    /**
     * Returns whether the response is acknowledged or not
     * @return true if the response is acknowledged, false otherwise
     */
    public final boolean isAcknowledged() {
        return acknowledged;
    }

    /**
     * Reads the timeout value
     */
    protected void readAcknowledged(StreamInput in) throws IOException {
        acknowledged = in.readBoolean();
    }

    /**
     * Writes the timeout value
     */
    protected void writeAcknowledged(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
    }

    protected void addAcknowledgedField(XContentBuilder builder) throws IOException {
        builder.field(ACKNOWLEDGED, isAcknowledged());
    }
}
