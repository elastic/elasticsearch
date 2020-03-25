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

package org.elasticsearch.client.license;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response class for license get basic status API
 */
public class GetBasicStatusResponse {

    private static final ParseField ELIGIBLE_TO_START_BASIC = new ParseField("eligible_to_start_basic");

    private static final ConstructingObjectParser<GetBasicStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_basic_status_response", true, a -> new GetBasicStatusResponse((boolean) a[0]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ELIGIBLE_TO_START_BASIC,
            ObjectParser.ValueType.BOOLEAN);
    }

    private final boolean eligibleToStartBasic;

    GetBasicStatusResponse(boolean eligibleToStartBasic) {
        this.eligibleToStartBasic = eligibleToStartBasic;
    }

    /**
     * Returns whether the license is eligible to start basic or not
     */
    public boolean isEligibleToStartBasic() {
        return eligibleToStartBasic;
    }

    public static GetBasicStatusResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetBasicStatusResponse that = (GetBasicStatusResponse) o;
        return eligibleToStartBasic == that.eligibleToStartBasic;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eligibleToStartBasic);
    }
}
