/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AzureOpenAiHeaderApplierTests extends ESTestCase {

    private static final String HEADER_NAME = "X-Test-Header";
    private static final String HEADER_VALUE = "test-value";

    public void testApplyTo_SetsHeaderOnRequest() {
        var header = new BasicHeader(HEADER_NAME, HEADER_VALUE);
        var applier = new AzureOpenAiHeaderApplier(() -> header);
        var request = new HttpPost();

        var listener = new TestPlainActionFuture<HttpRequestBase>();
        applier.applyTo(request, listener);

        var resultRequest = listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(resultRequest.getFirstHeader(HEADER_NAME).getValue(), is(HEADER_VALUE));
    }

    public void testApplyTo_InvokesListenerWithSameRequest() {
        var header = new BasicHeader(HEADER_NAME, HEADER_VALUE);
        var applier = new AzureOpenAiHeaderApplier(() -> header);
        var request = new HttpPost();

        var listener = new TestPlainActionFuture<HttpRequestBase>();
        applier.applyTo(request, listener);

        var resultRequest = listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(resultRequest, sameInstance(request));
    }
}
