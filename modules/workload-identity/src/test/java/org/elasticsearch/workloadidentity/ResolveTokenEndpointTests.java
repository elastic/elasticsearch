/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Focused unit tests for {@link HttpsWorkloadIdentityIssuerClient#resolveTokenEndpoint(String)}.
 *
 * <p>Covers the URI shaping rules (slash normalization, port/user-info preservation, scheme handling)
 * and the validation surface (null/empty input, malformed URI, non-{@code https} scheme).
 */
public class ResolveTokenEndpointTests extends ESTestCase {

    public void testAppendsTokenPathToRootHostUrl() {
        assertThat(resolve("https://issuer.example.com").toString(), equalTo("https://issuer.example.com/token"));
    }

    public void testAppendsTokenPathToRootHostUrlWithTrailingSlash() {
        assertThat(resolve("https://issuer.example.com/").toString(), equalTo("https://issuer.example.com/token"));
    }

    public void testAppendsTokenPathToBaseWithSubPath() {
        assertThat(resolve("https://issuer.example.com/api/v1").toString(), equalTo("https://issuer.example.com/api/v1/token"));
    }

    public void testStripsTrailingSlashFromBasePath() {
        assertThat(resolve("https://issuer.example.com/api/v1/").toString(), equalTo("https://issuer.example.com/api/v1/token"));
    }

    public void testStripsMultipleTrailingSlashesFromBasePath() {
        assertThat(resolve("https://issuer.example.com/api/v1///").toString(), equalTo("https://issuer.example.com/api/v1/token"));
    }

    public void testPreservesExplicitPort() {
        URI resolved = resolve("https://issuer.example.com:8443/api");
        assertThat(resolved.toString(), equalTo("https://issuer.example.com:8443/api/token"));
        assertThat(resolved.getPort(), equalTo(8443));
    }

    public void testPreservesUserInfo() {
        URI resolved = resolve("https://svc@issuer.example.com/api");
        assertThat(resolved.toString(), equalTo("https://svc@issuer.example.com/api/token"));
        assertThat(resolved.getUserInfo(), equalTo("svc"));
    }

    public void testRejectsQueryOnIssuerUrl() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("https://issuer.example.com/api?x=1"));
        assertThat(e.getMessage(), containsString("must not include a query string or fragment"));
    }

    public void testRejectsFragmentOnIssuerUrl() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("https://issuer.example.com/api#anchor"));
        assertThat(e.getMessage(), containsString("must not include a query string or fragment"));
    }

    public void testRejectsQueryAndFragmentOnIssuerUrl() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> resolve("https://issuer.example.com/api?x=1#anchor")
        );
        assertThat(e.getMessage(), containsString("must not include a query string or fragment"));
    }

    /**
     * The base path's percent-encoding must round-trip verbatim into the resolved endpoint. A
     * literal {@code %2F} in a segment is semantically distinct from an unencoded {@code /} for
     * any HTTP server that routes on raw path bytes (most production proxies do); silently
     * decoding it would point this client at a different endpoint.
     */
    public void testPreservesPercentEncodedBasePath() {
        URI resolved = resolve("https://issuer.example.com/api%2Fv1");
        assertThat(resolved.toString(), equalTo("https://issuer.example.com/api%2Fv1/token"));
        assertThat(resolved.getRawPath(), equalTo("/api%2Fv1/token"));
    }

    public void testHttpsSchemeIsMatchedCaseInsensitively() {
        // Case is preserved on the way out; the only relaxation is on the accept side.
        URI resolved = resolve("HTTPS://issuer.example.com/api");
        assertThat(resolved.toString(), equalTo("HTTPS://issuer.example.com/api/token"));
    }

    public void testRejectsNullIssuerUrl() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve(null));
        assertThat(e.getMessage(), containsString("setting [" + WorkloadIdentityIssuerSettings.ISSUER_URL_SETTING.getKey() + "]"));
        assertThat(e.getMessage(), containsString("must be configured"));
    }

    public void testRejectsEmptyIssuerUrl() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve(""));
        assertThat(e.getMessage(), containsString("must be configured"));
    }

    public void testRejectsMalformedUri() {
        // An unmatched IPv6 bracket is one of the few inputs the JDK URI parser reliably rejects;
        // looser issues (e.g., non-numeric ports, spaces) sometimes parse as opaque URIs depending
        // on JDK version.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("https://[invalid/api"));
        assertThat(e.getMessage(), containsString("is not a valid URI"));
        assertThat(e.getCause(), instanceOf(URISyntaxException.class));
    }

    public void testRejectsHttpScheme() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("http://issuer.example.com/api"));
        assertThat(e.getMessage(), containsString("must use the https scheme"));
    }

    public void testRejectsNonStandardScheme() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("ftp://issuer.example.com/api"));
        assertThat(e.getMessage(), containsString("must use the https scheme"));
    }

    public void testRejectsRelativeUriWithNoScheme() {
        // A scheme-less input parses as a relative URI; getScheme() returns null and we fail the https check.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("issuer.example.com/api"));
        assertThat(e.getMessage(), containsString("must use the https scheme"));
    }

    /**
     * An opaque URI like {@code https:relative/path} (no {@code //}) parses with scheme {@code https}
     * but a null host. Without an explicit host check it would pass the scheme gate and only fail
     * at the first dispatched request, far away from node startup.
     */
    public void testRejectsOpaqueHttpsUri() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("https:relative/path"));
        assertThat(e.getMessage(), containsString("must include a host"));
    }

    /** An empty authority ({@code https:///path}) leaves the host null. */
    public void testRejectsHttpsUriWithEmptyAuthority() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("https:///path"));
        assertThat(e.getMessage(), containsString("must include a host"));
    }

    /** A port without a host ({@code https://:8443/path}) also leaves the host null. */
    public void testRejectsHttpsUriWithPortButNoHost() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> resolve("https://:8443/path"));
        assertThat(e.getMessage(), containsString("must include a host"));
    }

    private static URI resolve(String issuerUrl) {
        return HttpsWorkloadIdentityIssuerClient.resolveTokenEndpoint(issuerUrl);
    }
}
