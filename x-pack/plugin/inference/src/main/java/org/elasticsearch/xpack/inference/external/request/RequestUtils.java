/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.SecureString;

public class RequestUtils {

    public static Header createAuthBearerHeader(SecureString apiKey) {
        return new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey.toString());
    }

    private RequestUtils() {}
}
