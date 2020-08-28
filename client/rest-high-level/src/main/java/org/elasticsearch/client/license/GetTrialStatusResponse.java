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
 * Response class for license get trial status API
 */
public class GetTrialStatusResponse {

    private static final ParseField ELIGIBLE_TO_START_TRIAL = new ParseField("eligible_to_start_trial");

    private static final ConstructingObjectParser<GetTrialStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_trial_status_response", true, a -> new GetTrialStatusResponse((boolean) a[0]));

    static {
        PARSER.declareField(constructorArg(), (parser, context) -> parser.booleanValue(), ELIGIBLE_TO_START_TRIAL,
            ObjectParser.ValueType.BOOLEAN);
    }

    private final boolean eligibleToStartTrial;

    GetTrialStatusResponse(boolean eligibleToStartTrial) {
        this.eligibleToStartTrial = eligibleToStartTrial;
    }

    /**
     * Returns whether the license is eligible to start trial or not
     */
    public boolean isEligibleToStartTrial() {
        return eligibleToStartTrial;
    }

    public static GetTrialStatusResponse fromXContent(XContentParser parser) {
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
        GetTrialStatusResponse that = (GetTrialStatusResponse) o;
        return eligibleToStartTrial == that.eligibleToStartTrial;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eligibleToStartTrial);
    }

}
