/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class JinaAIRequestTests extends ESTestCase {

    public void testDecorateWithAuthHeader() {
        var request = SimpleRequestBuilder.post("http://www.abc.com").build();

        JinaAIRequestUtils.decorateWithAuthHeader(request, new SecureString(new char[] { 'a', 'b', 'c' }));

        assertThat(request.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer abc"));
        assertThat(request.getFirstHeader(JinaAIUtils.REQUEST_SOURCE_HEADER).getValue(), is(JinaAIUtils.ELASTIC_REQUEST_SOURCE));
    }

}
