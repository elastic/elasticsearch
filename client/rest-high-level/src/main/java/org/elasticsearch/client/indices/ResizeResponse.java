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

package org.elasticsearch.client.indices;

import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.core.ShardsAcknowledgedResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response from a {@link ResizeRequest} call
 */
public class ResizeResponse extends ShardsAcknowledgedResponse {

    private static final ParseField INDEX = new ParseField("index");
    private static final ConstructingObjectParser<ResizeResponse, Void> PARSER = new ConstructingObjectParser<>("resize_index",
        true, args -> new ResizeResponse((boolean) args[0], (boolean) args[1], (String) args[2]));

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(AcknowledgedResponse.PARSE_FIELD_NAME));
        PARSER.declareBoolean(constructorArg(), new ParseField(SHARDS_PARSE_FIELD_NAME));
        PARSER.declareField(constructorArg(), (parser, context) -> parser.textOrNull(), INDEX, ObjectParser.ValueType.STRING_OR_NULL);
    }

    private final String index;

    public ResizeResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged);
        this.index = index;
    }

    public String index() {
        return index;
    }

    public static ResizeResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ResizeResponse that = (ResizeResponse) o;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }
}
