/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultAuthenticationFailureHandlerTests extends ESTestCase {

    public void testAuthenticationRequired() {
        final boolean testDefault = randomBoolean();
        final String basicAuthScheme = "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"";
        final String bearerAuthScheme = "Bearer realm=\"" + XPackField.SECURITY + "\"";
        final DefaultAuthenticationFailureHandler failureHandler;
        if (testDefault) {
            failureHandler = new DefaultAuthenticationFailureHandler(Collections.emptyMap());
        } else {
            final Map<String, List<String>> failureResponseHeaders = new HashMap<>();
            failureResponseHeaders.put("WWW-Authenticate", Arrays.asList(basicAuthScheme, bearerAuthScheme));
            failureHandler = new DefaultAuthenticationFailureHandler(failureResponseHeaders);
        }
        assertThat(failureHandler, is(notNullValue()));
        final ElasticsearchSecurityException ese =
                failureHandler.authenticationRequired("someaction", new ThreadContext(Settings.builder().build()));
        assertThat(ese, is(notNullValue()));
        assertThat(ese.getMessage(), equalTo("action [someaction] requires authentication"));
        assertThat(ese.getHeader("WWW-Authenticate"), is(notNullValue()));
        if (testDefault) {
            assertWWWAuthenticateWithSchemes(ese, basicAuthScheme);
        } else {
            assertWWWAuthenticateWithSchemes(ese, bearerAuthScheme, basicAuthScheme);
        }
    }

    public void testMissingToken() {
        final DefaultAuthenticationFailureHandler handler = new DefaultAuthenticationFailureHandler(Collections.emptyMap());
        final RestRequest request = mock(RestRequest.class);
        when(request.uri()).thenReturn("https://secret.es.shield.gov/");
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final ElasticsearchSecurityException ese = handler.missingToken(request, threadContext);
        assertThat(ese, is(notNullValue()));
        assertThat(ese.getMessage(), equalTo("missing authentication credentials for REST request [https://secret.es.shield.gov/]"));
    }

    public void testExceptionProcessingRequest() {
        final String basicAuthScheme = "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"";
        final String bearerAuthScheme = "Bearer realm=\"" + XPackField.SECURITY + "\"";
        final String negotiateAuthScheme = randomFrom("Negotiate", "Negotiate Ijoijksdk");
        final Map<String, List<String>> failureResponseHeaders = new HashMap<>();
        failureResponseHeaders.put("WWW-Authenticate", Arrays.asList(basicAuthScheme, bearerAuthScheme, negotiateAuthScheme));
        final DefaultAuthenticationFailureHandler failureHandler = new DefaultAuthenticationFailureHandler(failureResponseHeaders);

        assertThat(failureHandler, is(notNullValue()));
        final boolean causeIsElasticsearchSecurityException = randomBoolean();
        final boolean causeIsEseAndUnauthorized = causeIsElasticsearchSecurityException && randomBoolean();
        final ElasticsearchSecurityException eseCause = (causeIsEseAndUnauthorized)
                ? new ElasticsearchSecurityException("unauthorized", RestStatus.UNAUTHORIZED, null, (Object[]) null)
                : new ElasticsearchSecurityException("different error", RestStatus.BAD_REQUEST, null, (Object[]) null);
        final Exception cause = causeIsElasticsearchSecurityException ? eseCause : new Exception("other error");
        final boolean withAuthenticateHeader = randomBoolean();
        final String selectedScheme = randomFrom(bearerAuthScheme, basicAuthScheme, negotiateAuthScheme);
        if (withAuthenticateHeader) {
            eseCause.addHeader("WWW-Authenticate", Collections.singletonList(selectedScheme));
        }

        if (causeIsElasticsearchSecurityException) {
            if (causeIsEseAndUnauthorized) {
                final ElasticsearchSecurityException ese = failureHandler.exceptionProcessingRequest(mock(RestRequest.class), cause,
                        new ThreadContext(Settings.builder().build()));
                assertThat(ese, is(notNullValue()));
                assertThat(ese.getHeader("WWW-Authenticate"), is(notNullValue()));
                assertThat(ese, is(sameInstance(cause)));
                if (withAuthenticateHeader == false) {
                    assertWWWAuthenticateWithSchemes(ese, negotiateAuthScheme, bearerAuthScheme, basicAuthScheme);
                } else {
                    if (selectedScheme.contains("Negotiate ")) {
                        assertWWWAuthenticateWithSchemes(ese, selectedScheme);
                    } else {
                        assertWWWAuthenticateWithSchemes(ese, negotiateAuthScheme, bearerAuthScheme, basicAuthScheme);
                    }
                }
                assertThat(ese.getMessage(), equalTo("unauthorized"));
            } else {
                expectThrows(AssertionError.class, () -> failureHandler.exceptionProcessingRequest(mock(RestRequest.class), cause,
                        new ThreadContext(Settings.builder().build())));
            }
        } else {
            final ElasticsearchSecurityException ese = failureHandler.exceptionProcessingRequest(mock(RestRequest.class), cause,
                    new ThreadContext(Settings.builder().build()));
            assertThat(ese, is(notNullValue()));
            assertThat(ese.getHeader("WWW-Authenticate"), is(notNullValue()));
            assertThat(ese.getMessage(), equalTo("error attempting to authenticate request"));
            assertWWWAuthenticateWithSchemes(ese, negotiateAuthScheme, bearerAuthScheme, basicAuthScheme);
        }

    }

    public void testSortsWWWAuthenticateHeaderValues() {
        final String basicAuthScheme = "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"";
        final String bearerAuthScheme = "Bearer realm=\"" + XPackField.SECURITY + "\"";
        final String negotiateAuthScheme = randomFrom("Negotiate", "Negotiate Ijoijksdk");
        final String apiKeyAuthScheme = "ApiKey";
        final Map<String, List<String>> failureResponeHeaders = new HashMap<>();
        final List<String> supportedSchemes = Arrays.asList(basicAuthScheme, bearerAuthScheme, negotiateAuthScheme, apiKeyAuthScheme);
        Collections.shuffle(supportedSchemes, random());
        failureResponeHeaders.put("WWW-Authenticate", supportedSchemes);
        final DefaultAuthenticationFailureHandler failuerHandler = new DefaultAuthenticationFailureHandler(failureResponeHeaders);

        final ElasticsearchSecurityException ese = failuerHandler.exceptionProcessingRequest(mock(RestRequest.class), null,
                new ThreadContext(Settings.builder().build()));

        assertThat(ese, is(notNullValue()));
        assertThat(ese.getHeader("WWW-Authenticate"), is(notNullValue()));
        assertThat(ese.getMessage(), equalTo("error attempting to authenticate request"));
        assertWWWAuthenticateWithSchemes(ese, negotiateAuthScheme, bearerAuthScheme, apiKeyAuthScheme, basicAuthScheme);
    }

    private void assertWWWAuthenticateWithSchemes(final ElasticsearchSecurityException ese, final String... schemes) {
        assertThat(ese.getHeader("WWW-Authenticate").size(), is(schemes.length));
        assertThat(ese.getHeader("WWW-Authenticate"), contains(schemes));
    }
}
