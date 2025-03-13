/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.cohere.CohereAccount;
import org.elasticsearch.xpack.inference.external.request.Request;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public abstract class CohereRequest implements Request {

    public static void decorateWithAuthHeader(HttpPost request, CohereAccount account) {
        request.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        request.setHeader(createAuthBearerHeader(account.apiKey()));
        request.setHeader(CohereUtils.createRequestSourceHeader());
    }

}
