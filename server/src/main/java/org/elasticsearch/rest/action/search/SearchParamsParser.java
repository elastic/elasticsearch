/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.rest.RestRequest;

import java.util.Optional;

public class SearchParamsParser {
    public static final String MRT_SET_IN_CPS_WARN = "ccs_minimize_roundtrips always defaults to true in Cross Project Search context."
        + " Setting it explicitly has no effect irrespective of the value specified and is ignored."
        + " It will soon be deprecated and made unavailable for Cross project Search.";

    /**
     * For CPS, we do not necessarily want to use the MRT value that the user has provided.
     * Instead, we'd want to ignore it and default searches to `true` whilst issuing a warning
     * via the headers.
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
                request.param("ccs_minimize_roundtrips");
                HeaderWarning.addWarning(MRT_SET_IN_CPS_WARN);
                return true;
            }

            // MRT was not provided; default to true.
            return true;
        } else {
            // This is not a CPS request; use the value the user has provided.
            return request.paramAsBoolean("ccs_minimize_roundtrips", defaultValue);
        }
    }
}
