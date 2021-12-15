/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.common.io.stream.VersionCheckingStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.VersionUtils;

public class CCSVersionCheckHelper {

    public static String CCS_VERSION_CHECK_FLAG = "ccs_force_fail";

    public static void checkCCSVersionCompatibility(
        RestRequest request,
        Writeable searchRequest
    ) {
        if (request.paramAsBoolean(CCS_VERSION_CHECK_FLAG, false)) {
            // try serializing this request to a stream with previous minor version
            try {
                searchRequest.writeTo(new VersionCheckingStreamOutput(VersionUtils.getPreviousMinorAllVersions()));
            } catch (Exception e) {
                // if we cannot serialize, raise this as an error to indicate to the caller that CCS has problems with this request
                throw new IllegalArgumentException(
                    "request ["
                        + request.method()
                        + " "
                        + request.path()
                        + "] not serializable to previous minor and 'ccs_force_fail' enabled.",
                    e
                );
            }
        }
    }
}
