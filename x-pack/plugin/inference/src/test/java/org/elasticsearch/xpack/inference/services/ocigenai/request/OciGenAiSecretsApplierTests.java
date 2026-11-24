/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class OciGenAiSecretsApplierTests extends ESTestCase {

    private static final URI CHAT_URI = URI.create("https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/actions/chat");

    public void testApplyTo_SignsTheRequest() {
        var applier = new OciGenAiSecretsApplier(
            "id",
            OciGenAiTestUtils.createSecretSettings(),
            Clock.fixed(Instant.parse("2026-11-03T08:09:10Z"), ZoneOffset.UTC)
        );
        var request = createRequest();

        var listener = new PlainActionFuture<SimpleHttpRequest>();
        applier.applyTo(request, listener);
        var signed = listener.actionGet();

        assertThat(signed, sameInstance(request));
        assertThat(signed.getFirstHeader("date").getValue(), is("Tue, 03 Nov 2026 08:09:10 GMT"));
        assertThat(signed.getFirstHeader("host").getValue(), is("inference.generativeai.us-chicago-1.oci.oraclecloud.com"));
        assertThat(
            signed.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(),
            startsWith("Signature version=\"1\",keyId=\"" + OciGenAiTestUtils.keyId() + "\"")
        );
    }

    public void testApplyTo_FailsWithoutSecrets() {
        var applier = new OciGenAiSecretsApplier("id", null);

        var listener = new PlainActionFuture<SimpleHttpRequest>();
        applier.applyTo(createRequest(), listener);

        var exception = expectThrows(ElasticsearchStatusException.class, listener::actionGet);
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(
            exception.getMessage(),
            is("Unable to sign the OCI Generative AI request from inference entity id [id] because the API signing key is missing")
        );
    }

    public void testApplyTo_FailsWhenTheStoredKeyDoesNotParse() {
        // the key is validated when the endpoint is created, so this only happens with a corrupted stored secret
        var applier = new OciGenAiSecretsApplier("id", OciGenAiTestUtils.createSecretSettings("not a pem"));

        var listener = new PlainActionFuture<SimpleHttpRequest>();
        applier.applyTo(createRequest(), listener);

        var exception = expectThrows(ElasticsearchStatusException.class, listener::actionGet);
        assertThat(exception.status(), is(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("because the API signing key is invalid"));
    }

    private static SimpleHttpRequest createRequest() {
        var request = SimpleRequestBuilder.post(CHAT_URI).build();
        request.setBody("{}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        return request;
    }
}
