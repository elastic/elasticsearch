/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.services.custom.request.CustomRequest;

import java.io.IOException;

public class CustomResponseEntity {
    public static InferenceServiceResults fromResponse(OutboundRequest outboundRequest, HttpResult response) throws IOException {
        if (outboundRequest instanceof CustomRequest customRequest) {
            var responseJsonParser = customRequest.getServiceSettings().getResponseJsonParser();

            return responseJsonParser.parse(response);
        } else {
            throw new IllegalArgumentException(
                Strings.format(
                    "Original request is an invalid type [%s], expected [%s]",
                    outboundRequest.getClass().getSimpleName(),
                    CustomRequest.class.getSimpleName()
                )
            );
        }
    }
}
