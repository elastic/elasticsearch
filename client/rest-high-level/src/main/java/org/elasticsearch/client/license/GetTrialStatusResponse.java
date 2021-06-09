/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.license;

import org.elasticsearch.common.xcontent.ParseField;
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
