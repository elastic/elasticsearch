/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_CREDENTIALS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_HEADERS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_METHODS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_CORS_MAX_AGE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class CorsHandlerTests extends ESTestCase {

    public void testCorsConfigWithBadRegex() {
        final Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "/[*/")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        SettingsException e = expectThrows(SettingsException.class, () -> CorsHandler.buildConfig(settings));
        assertThat(e.getMessage(), containsString("Bad regex in [http.cors.allow-origin]: [/[*/]"));
        assertThat(e.getCause(), instanceOf(PatternSyntaxException.class));
    }

    public void testCorsConfig() {
        final Set<String> methods = new HashSet<>(Arrays.asList("get", "options", "post"));
        final Set<String> headers = new HashSet<>(Arrays.asList("Content-Type", "Content-Length"));
        final String prefix = randomBoolean() ? " " : ""; // sometimes have a leading whitespace between comma delimited elements
        final Settings settings = Settings.builder()
            .put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), collectionToDelimitedString(methods, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_HEADERS.getKey(), collectionToDelimitedString(headers, ",", prefix, ""))
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        final CorsHandler.Config corsConfig = CorsHandler.buildConfig(settings);
        assertTrue(corsConfig.isAnyOriginSupported());
        assertEquals(headers, corsConfig.allowedRequestHeaders());
        assertEquals(
            methods.stream().map(s -> s.toUpperCase(Locale.ENGLISH)).collect(Collectors.toSet()),
            corsConfig.allowedRequestMethods().stream().map(RestRequest.Method::name).collect(Collectors.toSet())
        );
    }

    public void testCorsConfigWithDefaults() {
        final Set<String> methods = Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_METHODS.getDefault(Settings.EMPTY));
        final Set<String> headers = Strings.commaDelimitedListToSet(SETTING_CORS_ALLOW_HEADERS.getDefault(Settings.EMPTY));
        final long maxAge = SETTING_CORS_MAX_AGE.getDefault(Settings.EMPTY);
        final Settings settings = Settings.builder().put(SETTING_CORS_ENABLED.getKey(), true).build();
        final CorsHandler.Config corsConfig = CorsHandler.buildConfig(settings);
        assertFalse(corsConfig.isAnyOriginSupported());
        assertEquals(Collections.emptySet(), corsConfig.origins().get());
        assertEquals(headers, corsConfig.allowedRequestHeaders());
        assertEquals(methods, corsConfig.allowedRequestMethods().stream().map(RestRequest.Method::name).collect(Collectors.toSet()));
        assertEquals(maxAge, corsConfig.maxAge());
        assertFalse(corsConfig.isCredentialsAllowed());
    }

    public void testHandleInboundNonCorsRequest() {
        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true).build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);
        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        HttpResponse httpResponse = corsHandler.handleInbound(request);
        // Since this is not a Cors request, there is not an early response
        assertThat(httpResponse, nullValue());
    }

    public void testHandleInboundValidCorsRequest() {
        final String validOriginLiteral = "valid-origin";
        final String originSetting;
        if (randomBoolean()) {
            originSetting = validOriginLiteral;
        } else {
            if (randomBoolean()) {
                originSetting = "/valid-.+/";
            } else {
                originSetting = "*";
            }
        }
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originSetting)
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);
        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.POST, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList(validOriginLiteral));
        HttpResponse httpResponse = corsHandler.handleInbound(request);
        // Since is a Cors enabled request. However, it is not forbidden because the origin is allowed.
        assertThat(httpResponse, nullValue());
    }

    public void testHandleInboundForbidden() {
        final String validOriginLiteral = "valid-origin";
        final String originSetting;
        if (randomBoolean()) {
            originSetting = validOriginLiteral;
        } else {
            originSetting = "/valid-.+/";
        }
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originSetting)
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);
        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.POST, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("invalid-origin"));
        TestHttpResponse httpResponse = (TestHttpResponse) corsHandler.handleInbound(request);
        // Forbidden
        assertThat(httpResponse.status(), equalTo(RestStatus.FORBIDDEN));
    }

    public void testHandleInboundAllowsSameOrigin() {
        final String validOriginLiteral = "valid-origin";
        final String originSetting;
        if (randomBoolean()) {
            originSetting = validOriginLiteral;
        } else {
            originSetting = "/valid-.+/";
        }
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), originSetting)
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);
        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.POST, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("https://same-host"));
        request.getHeaders().put(CorsHandler.HOST, Collections.singletonList("same-host"));
        TestHttpResponse httpResponse = (TestHttpResponse) corsHandler.handleInbound(request);
        // Since is a Cors enabled request. However, it is not forbidden because the origin is the same as the host.
        assertThat(httpResponse, nullValue());
    }

    public void testHandleInboundPreflightWithWildcardNoCredentials() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), "OPTIONS,HEAD,GET,DELETE")
            .put(SETTING_CORS_ALLOW_HEADERS.getKey(), "Content-Type,Content-Length")
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);
        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.OPTIONS, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("valid-origin"));
        request.getHeaders().put(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, Collections.singletonList("POST"));
        TestHttpResponse httpResponse = (TestHttpResponse) corsHandler.handleInbound(request);

        assertThat(httpResponse.status(), equalTo(RestStatus.OK));
        Map<String, List<String>> headers = httpResponse.headers();
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), containsInAnyOrder("*"));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_METHODS), containsInAnyOrder("HEAD", "OPTIONS", "GET", "DELETE"));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_HEADERS), containsInAnyOrder("Content-Type", "Content-Length"));
        assertNull(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_MAX_AGE), containsInAnyOrder("1728000"));
        assertNotNull(headers.get(CorsHandler.DATE));
    }

    public void testHandleInboundPreflightWithWildcardAllowCredentials() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), "OPTIONS,HEAD,GET,DELETE,POST")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);
        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.OPTIONS, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("valid-origin"));
        request.getHeaders().put(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, Collections.singletonList("POST"));
        TestHttpResponse httpResponse = (TestHttpResponse) corsHandler.handleInbound(request);

        assertThat(httpResponse.status(), equalTo(RestStatus.OK));
        Map<String, List<String>> headers = httpResponse.headers();
        // Since credentials are allowed, we echo the origin
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), containsInAnyOrder("valid-origin"));
        assertThat(headers.get(CorsHandler.VARY), containsInAnyOrder(CorsHandler.ORIGIN));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_METHODS), containsInAnyOrder("HEAD", "OPTIONS", "GET", "DELETE", "POST"));
        assertThat(
            headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_HEADERS),
            containsInAnyOrder("X-Requested-With", "Content-Type", "Content-Length")
        );
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS), containsInAnyOrder("true"));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_MAX_AGE), containsInAnyOrder("1728000"));
        assertNotNull(headers.get(CorsHandler.DATE));
    }

    public void testHandleInboundPreflightWithValidOriginAllowCredentials() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "valid-origin")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), "OPTIONS,HEAD,GET,DELETE,POST")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);
        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.OPTIONS, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("valid-origin"));
        request.getHeaders().put(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, Collections.singletonList("POST"));
        TestHttpResponse httpResponse = (TestHttpResponse) corsHandler.handleInbound(request);

        assertThat(httpResponse.status(), equalTo(RestStatus.OK));
        Map<String, List<String>> headers = httpResponse.headers();
        // Since credentials are allowed, we echo the origin
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), containsInAnyOrder("valid-origin"));
        assertThat(headers.get(CorsHandler.VARY), containsInAnyOrder(CorsHandler.ORIGIN));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_METHODS), containsInAnyOrder("HEAD", "OPTIONS", "GET", "DELETE", "POST"));
        assertThat(
            headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_HEADERS),
            containsInAnyOrder("X-Requested-With", "Content-Type", "Content-Length")
        );
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS), containsInAnyOrder("true"));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_MAX_AGE), containsInAnyOrder("1728000"));
        assertNotNull(headers.get(CorsHandler.DATE));
    }

    public void testSetResponseNonCorsRequest() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_METHODS.getKey(), "OPTIONS,HEAD,GET,DELETE")
            .put(SETTING_CORS_ALLOW_HEADERS.getKey(), "Content-Type,Content-Length")
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);

        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        TestHttpResponse response = new TestHttpResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);

        Map<String, List<String>> headers = response.headers();
        assertNull(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN));
    }

    public void testSetResponseHeadersWithWildcardOrigin() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);

        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("valid-origin"));
        TestHttpResponse response = new TestHttpResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);

        Map<String, List<String>> headers = response.headers();
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), containsInAnyOrder("*"));
        assertNull(headers.get(CorsHandler.VARY));
    }

    public void testSetResponseHeadersWithCredentialsWithWildcard() {
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "*")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), true)
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);

        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("valid-origin"));
        TestHttpResponse response = new TestHttpResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);

        Map<String, List<String>> headers = response.headers();
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), containsInAnyOrder("valid-origin"));
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS), containsInAnyOrder("true"));
        assertThat(headers.get(CorsHandler.VARY), containsInAnyOrder(CorsHandler.ORIGIN));
    }

    public void testSetResponseHeadersWithNonWildcardOrigin() {
        boolean allowCredentials = randomBoolean();
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "valid-origin")
            .put(SETTING_CORS_ALLOW_CREDENTIALS.getKey(), allowCredentials)
            .build();
        CorsHandler corsHandler = CorsHandler.fromSettings(settings);

        TestHttpRequest request = new TestHttpRequest(HttpRequest.HttpVersion.HTTP_1_1, RestRequest.Method.GET, "/");
        request.getHeaders().put(CorsHandler.ORIGIN, Collections.singletonList("valid-origin"));
        TestHttpResponse response = new TestHttpResponse(RestStatus.OK, BytesArray.EMPTY);
        corsHandler.setCorsResponseHeaders(request, response);

        Map<String, List<String>> headers = response.headers();
        assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), containsInAnyOrder("valid-origin"));
        assertThat(headers.get(CorsHandler.VARY), containsInAnyOrder(CorsHandler.ORIGIN));
        if (allowCredentials) {
            assertThat(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS), containsInAnyOrder("true"));
        } else {
            assertNull(headers.get(CorsHandler.ACCESS_CONTROL_ALLOW_CREDENTIALS));
        }
    }
}
