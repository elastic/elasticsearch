/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.rest.RestRequest;

import java.util.Optional;

public class SearchParamsParser {
    public static final String MRT_SET_IN_CPS_WARN =
        "ccs_minimize_roundtrips defaults to true in Cross Project Search context when omitted.";

    /**
     * Parses ccs_minimize_roundtrips, defaulting CPS requests to true only when the user does not specify a value.
     * @param crossProjectEnabled If running in Cross Project Search environment.
     * @param request Rest request that we're parsing.
     * @return A boolean that determines if round trips should be minimised for this search request.
     */
    public static boolean parseCcsMinimizeRoundtrips(Optional<Boolean> crossProjectEnabled, RestRequest request) {
        return parseCcsMinimizeRoundtrips(crossProjectEnabled, request, true);
    }

    public static boolean parseCcsMinimizeRoundtrips(Optional<Boolean> crossProjectEnabled, RestRequest request, boolean defaultValue) {
        if (crossProjectEnabled.orElse(false)) {
            if (request.hasParam("ccs_minimize_roundtrips")) {
                return request.paramAsBoolean("ccs_minimize_roundtrips", defaultValue);
            }

            // MRT was not provided; default to true.
            return true;
        } else {
            // This is not a CPS request; use the value the user has provided.
            return request.paramAsBoolean("ccs_minimize_roundtrips", defaultValue);
        }
    }
}
