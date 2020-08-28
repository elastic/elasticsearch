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
package org.elasticsearch.client.core;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class ShardsAcknowledgedResponse extends AcknowledgedResponse {

    protected static final String SHARDS_PARSE_FIELD_NAME = "shards_acknowledged";
    private static ConstructingObjectParser<ShardsAcknowledgedResponse, Void> buildParser() {

        ConstructingObjectParser<ShardsAcknowledgedResponse, Void> p = new ConstructingObjectParser<>("freeze", true,
            args -> new ShardsAcknowledgedResponse((boolean) args[0], (boolean) args[1]));
        p.declareBoolean(constructorArg(), new ParseField(AcknowledgedResponse.PARSE_FIELD_NAME));
        p.declareBoolean(constructorArg(), new ParseField(SHARDS_PARSE_FIELD_NAME));
        return p;
    }

    private static final ConstructingObjectParser<ShardsAcknowledgedResponse, Void> PARSER = buildParser();

    private final boolean shardsAcknowledged;

    public ShardsAcknowledgedResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged);
        this.shardsAcknowledged = shardsAcknowledged;
    }

    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }

    public static ShardsAcknowledgedResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
