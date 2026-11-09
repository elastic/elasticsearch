/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.cohere.CohereAccount;

import java.net.URI;

import static org.hamcrest.Matchers.is;

public class CohereRequestTests extends ESTestCase {

    public void testDecorateWithAuthHeader() {
        var request = SimpleRequestBuilder.post("http://www.abc.com").build();

        CohereRequest.decorateWithAuthHeader(
            request,
            new CohereAccount(URI.create("http://www.abc.com"), new SecureString(new char[] { 'a', 'b', 'c' }))
        );

        assertThat(request.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer abc"));
        assertThat(request.getFirstHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(), is(CohereUtils.ELASTIC_REQUEST_SOURCE));
    }
}
