/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.VersionCheckingStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestRequest;

/**
 * Using the 'check_ccs_compatibility' on rest requests, clients can ask for an early
 * check that inspects the incoming request and tries to verify that it can be handled by
 * a CCS compliant earlier version, e.g. currently a N-1 version where N is the current minor.
 *
 * Checking the compatibility involved serializing the request to a stream output that acts like
 * it was on the previous minor version. This should e.g. trigger errors for {@link Writeable} parts of
 * the requests that were of available on those versions.
 */
public class CCSVersionCheckHelper {

    public static final String CCS_VERSION_CHECK_FLAG = "check_ccs_compatibility";
    public static final Version checkVersion = Version.CURRENT.previousFirstMinor();

    /**
     * run the writeableRequest through serialization to the previous minor version if the 'check_ccs_compatibility' is set on the request
     */
    public static void checkCCSVersionCompatibility(RestRequest request, Writeable writeableRequest) {
        if (request.paramAsBoolean(CCS_VERSION_CHECK_FLAG, false)) {
            // try serializing this request to a stream with previous minor version
            try {
                writeableRequest.writeTo(new VersionCheckingStreamOutput(checkVersion));
            } catch (Exception e) {
                // if we cannot serialize, raise this as an error to indicate to the caller that CCS has problems with this request
                throw new IllegalArgumentException(
                    "parts of request ["
                        + request.method()
                        + " "
                        + request.path()
                        + "] are not compatible with version "
                        + checkVersion
                        + " and the '"
                        + CCS_VERSION_CHECK_FLAG
                        + "' is enabled.",
                    e
                );
            }
        }
    }

    /**
     * run the writeableRequest through serialization to the previous minor version
     */
    public static void checkCCSVersionCompatibility(Writeable writeableRequest) {
        try {
            writeableRequest.writeTo(new VersionCheckingStreamOutput(checkVersion));
        } catch (Exception e) {
            // if we cannot serialize, raise this as an error to indicate to the caller that CCS has problems with this request
            throw new IllegalArgumentException(
                "parts of writeable ["
                    + writeableRequest
                    + "] are not compatible with version "
                    + checkVersion
                    + " and the '"
                    + CCS_VERSION_CHECK_FLAG
                    + "' is enabled.",
                e
            );
        }
    }
}
