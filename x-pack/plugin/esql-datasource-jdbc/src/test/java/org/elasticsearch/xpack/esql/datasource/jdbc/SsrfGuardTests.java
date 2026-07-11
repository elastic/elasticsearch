/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

/**
 * Behavioural tests for {@link SsrfGuard}. Each test pins one scenario from the production threat model:
 * the cloud metadata endpoint, IPv6 loopback, file-backed JDBC subprotocols, etc. A passing suite is the
 * primary guarantee that future contributors cannot loosen the guard accidentally.
 */
public class SsrfGuardTests extends ESTestCase {

    private final SsrfGuard guard = SsrfGuard.defaultGuard();

    public void testAllowsConfiguredPostgresUrl() {
        assertAllowed("jdbc:postgresql://db.corp.internal:5432/orders");
    }

    public void testAllowsConfiguredMySqlUrl() {
        assertAllowed("jdbc:mysql://db.corp.internal:3306/orders");
    }

    public void testAllowsConfiguredRedshiftUrl() {
        // The dedicated Amazon Redshift scheme is in the default allowlist. Host filters still apply (this
        // is a routable cluster endpoint, not loopback/link-local), so it passes end-to-end.
        assertAllowed("jdbc:redshift://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/orders");
    }

    public void testRedshiftStillSubjectToHostFilters() {
        // Adding the scheme to the allowlist must not exempt it from the loopback / link-local host rejection.
        assertFalse(guard.evaluate("jdbc:redshift://169.254.169.254:5439/x").allowed());
        assertFalse(guard.evaluate("jdbc:redshift://127.0.0.1:5439/x").allowed());
    }

    // -- Redshift IAM sub-scheme --

    public void testAllowsRedshiftIamHostPortForm() {
        // jdbc:redshift:iam://<host>:<port>/db -- a normal network host reached over a real port. This must be
        // allowlisted (it does NOT prefix-match the plain jdbc:redshift:// entry) and pass the normal host filter.
        assertAllowed("jdbc:redshift:iam://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/orders");
    }

    public void testAllowsRedshiftIamClusterIdRegionForm() {
        // jdbc:redshift:iam://<cluster-id>:<region>/db -- here the ":<region>" is NOT a TCP port and the real
        // endpoint is AWS-resolved by the driver, so the guard vets only the scheme + the extracted cluster-id
        // token (a plain identifier, neither an IP nor a known-bad name). It must NOT be rejected as an
        // invalid-port/loopback host.
        assertAllowed("jdbc:redshift:iam://my-redshift-cluster:us-east-1/orders");
        assertAllowed("jdbc:redshift:iam://my-redshift-cluster:eu-west-2/db");
    }

    public void testRedshiftIamSchemeNotBlockedByPlainRedshiftPrefix() {
        // Guards the research finding: jdbc:redshift:iam://... does not startsWith jdbc:redshift:// so without its
        // own allowlist entry it would be BLOCKED. It must be allowed (and its own entry present).
        assertTrue(guard.evaluate("jdbc:redshift:iam://cluster.x.us-east-1.redshift.amazonaws.com:5439/db").allowed());
        assertTrue(SsrfGuard.DEFAULT_ALLOWED_SUBPROTOCOLS.contains("jdbc:redshift:iam:"));
    }

    public void testRedshiftIamHostFormStillSubjectToHostFilters() {
        // The host:port form still gets the normal loopback / link-local rejection (defense-in-depth); only the
        // cluster-id:region form is exempt from meaningful host checks (its token is not a network host).
        assertFalse(guard.evaluate("jdbc:redshift:iam://127.0.0.1:5439/x").allowed());
        assertFalse(guard.evaluate("jdbc:redshift:iam://169.254.169.254:5439/x").allowed());
    }

    public void testRedshiftIamSchemeIsCaseInsensitive() {
        assertTrue(guard.evaluate("JDBC:REDSHIFT:IAM://my-cluster:us-east-1/db").allowed());
    }

    public void testMalformedAuthorityFormStaysFailClosed() {
        // The leading-'//' strip is scoped to opaque/':'-terminated prefixes. An authority-form prefix that already
        // ends in '//' (jdbc:postgresql://) followed by a stray extra '//' is a MALFORMED authority: it must NOT be
        // silently repaired into a checkable host but denied conservatively (fail-closed).
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql:////evil.internal/x");
        assertFalse("malformed authority must be denied, not repaired", d.allowed());
        assertThat(d.reason(), org.hamcrest.Matchers.containsString("could not parse a host"));
    }

    public void testAllowsOracleThinAtForm() {
        assertAllowed("jdbc:oracle:thin:@db.corp.internal:1521:ORCL");
    }

    public void testAllowsOracleEzConnectAtSlashSlashForm() {
        // Oracle EZConnect jdbc:oracle:thin:@//host:port/service: the ':'-terminated prefix means the leading '//'
        // after the '@' is a real authority marker and IS stripped, so the host is extracted and filtered normally.
        assertAllowed("jdbc:oracle:thin:@//db.corp.internal:1521/ORCLPDB1");
    }

    public void testAllowsOracleThinWithUserInfo() {
        // Oracle accepts user/pass before the @host; sanitizer fields stay generic; just check the guard's host
        // extraction follows the @ that separates userinfo from host.
        assertAllowed("jdbc:oracle:thin:scott/tiger@db.corp.internal:1521:ORCL");
    }

    public void testAllowsH2InMemory() {
        assertAllowed("jdbc:h2:mem:test;DATABASE_TO_UPPER=false");
    }

    public void testRejectsH2FileSubprotocol() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:h2:file:/tmp/leak.db");
        assertFalse(d.allowed());
        assertThat(d.reason(), org.hamcrest.Matchers.containsString("subprotocol is not in the allowlist"));
    }

    public void testRejectsDerbyDirectorySubprotocol() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:derby:directory:/tmp/db");
        assertFalse(d.allowed());
    }

    public void testRejectsUnknownSubprotocol() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:bogus://internal/x");
        assertFalse(d.allowed());
    }

    public void testRejectsNullAndEmpty() {
        assertFalse(guard.evaluate(null).allowed());
        assertFalse(guard.evaluate("").allowed());
    }

    public void testRejectsLoopbackV4ByDefault() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://127.0.0.1:5432/x");
        assertFalse(d.allowed());
        assertThat(d.reason(), org.hamcrest.Matchers.containsString("loopback"));
    }

    public void testRejectsLoopbackV4OctetVariant() {
        // 127.0.0.5 is still 127.0.0.0/8 -- isLoopbackAddress covers the whole range.
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://127.0.0.5:5432/x");
        assertFalse(d.allowed());
    }

    public void testRejectsLoopbackV6() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://[::1]:5432/x");
        assertFalse(d.allowed());
    }

    public void testRejectsLocalhostByName() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://localhost:5432/x");
        assertFalse(d.allowed());
    }

    public void testRejectsLocalhostLocaldomainByName() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://localhost.localdomain:5432/x");
        assertFalse(d.allowed());
    }

    public void testRejectsCloudMetadataIp() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://169.254.169.254:80/x");
        assertFalse(d.allowed());
        assertThat(d.reason(), org.hamcrest.Matchers.containsString("link-local"));
    }

    public void testRejectsLinkLocalIPv6() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://[fe80::1]:5432/x");
        assertFalse(d.allowed());
    }

    public void testRejectsGoogleMetadataName() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://metadata.google.internal/db");
        assertFalse(d.allowed());
    }

    public void testRejectsWildcardAddress() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://0.0.0.0:5432/x");
        assertFalse(d.allowed());
    }

    public void testRejectsMulticast() {
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://224.0.0.1:5432/x");
        assertFalse(d.allowed());
    }

    public void testAllowsLoopbackWhenConfigured() {
        SsrfGuard relaxed = new SsrfGuard(SsrfGuard.DEFAULT_ALLOWED_SUBPROTOCOLS, true);
        assertTrue(relaxed.evaluate("jdbc:postgresql://127.0.0.1:5432/x").allowed());
        assertTrue(relaxed.evaluate("jdbc:postgresql://[::1]:5432/x").allowed());
        assertTrue(relaxed.evaluate("jdbc:postgresql://localhost:5432/x").allowed());
        // Link-local stays denied even when loopback is allowed -- that's the whole point.
        assertFalse(relaxed.evaluate("jdbc:postgresql://169.254.169.254:80/x").allowed());
    }

    public void testAllowsUserConfiguredSubprotocol() {
        SsrfGuard withDb2 = new SsrfGuard(List.of("jdbc:db2://"), false);
        assertTrue(withDb2.evaluate("jdbc:db2://db.corp.internal:50000/orders").allowed());
        // Things outside the new list are blocked, even if they were in the default allowlist.
        assertFalse(withDb2.evaluate("jdbc:postgresql://db.corp.internal/x").allowed());
    }

    public void testSubprotocolMatchingIsCaseInsensitive() {
        assertTrue(guard.evaluate("JDBC:POSTGRESQL://db.corp.internal/x").allowed());
    }

    public void testParseAcceptsCsvAndTrims() {
        SsrfGuard parsed = SsrfGuard.parse("jdbc:postgresql://, jdbc:mysql:// , ,jdbc:custom://", false);
        assertEquals(3, parsed.allowedSubprotocols().size());
        assertTrue(parsed.allowedSubprotocols().contains("jdbc:postgresql://"));
        assertTrue(parsed.allowedSubprotocols().contains("jdbc:mysql://"));
        assertTrue(parsed.allowedSubprotocols().contains("jdbc:custom://"));
    }

    public void testParseEmptyFallsBackToDefault() {
        SsrfGuard parsed = SsrfGuard.parse("", false);
        assertEquals(SsrfGuard.DEFAULT_ALLOWED_SUBPROTOCOLS, parsed.allowedSubprotocols());
    }

    public void testParseNullFallsBackToDefault() {
        SsrfGuard parsed = SsrfGuard.parse(null, false);
        assertEquals(SsrfGuard.DEFAULT_ALLOWED_SUBPROTOCOLS, parsed.allowedSubprotocols());
    }

    public void testToStringHasUsefulFields() {
        String s = guard.toString();
        assertThat(s, org.hamcrest.Matchers.containsString("allowed="));
        assertThat(s, org.hamcrest.Matchers.containsString("allowLoopback=false"));
    }

    public void testRejectsAllowedSubprotocolButUnparseableHost() {
        // Allowed subprotocol, but the rest doesn't actually contain a host (e.g. someone forgot the // form).
        SsrfGuard.Decision d = guard.evaluate("jdbc:postgresql://");
        assertFalse(d.allowed());
        assertThat(d.reason(), org.hamcrest.Matchers.containsString("could not parse a host"));
    }

    private void assertAllowed(String url) {
        SsrfGuard.Decision d = guard.evaluate(url);
        assertTrue("expected ALLOWED for [" + url + "], got " + d.reason(), d.allowed());
        assertNull(d.reason());
    }
}
