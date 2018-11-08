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
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class AcknowledgedResponse {

    protected static final String PARSE_FIELD_NAME = "acknowledged";
    private static final ConstructingObjectParser<AcknowledgedResponse, Void> PARSER = AcknowledgedResponse
        .generateParser("acknowledged_response", AcknowledgedResponse::new, AcknowledgedResponse.PARSE_FIELD_NAME);

    private final boolean acknowledged;

    public AcknowledgedResponse(final boolean acknowledged) {
        this.acknowledged = acknowledged;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    protected static <T> ConstructingObjectParser<T, Void> generateParser(String name, Function<Boolean, T> ctor, String parseField) {
        ConstructingObjectParser<T, Void> p = new ConstructingObjectParser<>(name, true, args -> ctor.apply((boolean) args[0]));
        p.declareBoolean(constructorArg(), new ParseField(parseField));
        return p;
    }

    public static AcknowledgedResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AcknowledgedResponse that = (AcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged();
    }

    @Override
    public int hashCode() {
        return Objects.hash(acknowledged);
    }

    /**
     * @return the field name this response uses to output the acknowledged flag
     */
    protected String getFieldName() {
        return PARSE_FIELD_NAME;
    }
}
