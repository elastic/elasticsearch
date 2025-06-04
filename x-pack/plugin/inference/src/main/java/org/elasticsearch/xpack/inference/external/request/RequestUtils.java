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
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;

import java.net.URI;
import java.net.URISyntaxException;

public class RequestUtils {

    public static Header createAuthBearerHeader(SecureString apiKey) {
        return new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey.toString());
    }

    public static URI buildUri(URI accountUri, String service, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        try {
            return accountUri == null ? uriBuilder.get() : accountUri;
        } catch (URISyntaxException e) {
            // using bad request here so that potentially sensitive URL information does not get logged
            throw new ElasticsearchStatusException(Strings.format("Failed to construct %s URL", service), RestStatus.BAD_REQUEST, e);
        }
    }

    public static URI buildUri(String service, CheckedSupplier<URI, URISyntaxException> uriBuilder) {
        return buildUri(null, service, uriBuilder);
    }

    private RequestUtils() {}
}
