/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransportRelaySingleNodeTests extends SecuritySingleNodeTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testAuthenticateRequest() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        AuthenticateRequest.INSTANCE.writeTo(out);
        final String payload = Base64.getEncoder().encodeToString(out.bytes().array());

        final Response response = performRelayRequest(AuthenticateAction.NAME, payload);
        final ByteArrayStreamInput in = wrapRelayResponse(response);

        final Authentication authentication = new AuthenticateResponse(in).authentication();
        assertThat(authentication.getEffectiveSubject().getUser().principal(), equalTo(SecuritySettingsSource.TEST_USER_NAME));
    }

    public void testSearch() throws IOException {
        client().prepareIndex("index")
            .setSource(Map.of("foo", "bar"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute()
            .actionGet();

        final BytesStreamOutput out = new BytesStreamOutput();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(QueryBuilders.matchAllQuery());
        final SearchRequest searchRequest = new SearchRequest(new String[] { "*" }, source);
        searchRequest.writeTo(out);
        final String payload = Base64.getEncoder().encodeToString(out.bytes().array());

        final Response response = performRelayRequest(SearchAction.NAME, payload);
        final ByteArrayStreamInput in = wrapRelayResponse(response);

        final SearchResponse searchResponse = new SearchResponse(in);
        assertThat(searchResponse.status().getStatus(), equalTo(200));
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    private Response performRelayRequest(String actionName, String payload) throws IOException {
        final Request request = new Request("POST", "_transport_relay");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader(
                    "Authorization",
                    UsernamePasswordToken.basicAuthHeaderValue(
                        SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
                    )
                )
        );
        request.setJsonEntity("""
            {
              "action": "%s",
              "payload": "%s"
            }""".formatted(actionName, payload));
        final Response response = getRestClient().performRequest(request);
        return response;
    }

    private static ByteArrayStreamInput wrapRelayResponse(Response response) throws IOException {
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final String body = objectPath.evaluate("body").toString();
        final ByteArrayStreamInput in = new ByteArrayStreamInput(Base64.getDecoder().decode(body));
        return in;
    }

}
