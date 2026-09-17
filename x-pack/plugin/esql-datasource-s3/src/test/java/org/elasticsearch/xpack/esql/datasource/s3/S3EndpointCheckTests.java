/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointParams;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;
import software.amazon.awssdk.services.sts.endpoints.StsEndpointParams;
import software.amazon.awssdk.services.sts.endpoints.StsEndpointProvider;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

/**
 * The endpoint constraint, driven directly rather than through the validator.
 *
 * <p>{@link #testAcceptsEveryEndpointTheResolverProduces} is the load-bearing one: it asks the SDK's own
 * endpoint resolver for the destination of every region/FIPS/dual-stack combination across all eight AWS
 * partitions and requires the rule to accept each. A rule written against a single literal suffix cannot
 * pass it, and a partition or variant the SDK gains is covered without editing this file.
 */
public class S3EndpointCheckTests extends ESTestCase {

    /** One region per partition, plus extras in the commercial partition. */
    private static final List<String> REGIONS = List.of(
        "us-east-1",
        "eu-west-1",
        "ap-southeast-2",
        "cn-north-1",
        "us-gov-west-1",
        "us-iso-east-1",
        "us-isob-east-1",
        "eu-isoe-west-1",
        "us-isof-south-1",
        "eusc-de-east-1"
    );

    public void testAcceptsEveryEndpointTheResolverProduces() {
        int checked = 0;
        for (String region : REGIONS) {
            for (boolean fips : List.of(false, true)) {
                for (boolean dualStack : List.of(false, true)) {
                    String s3Host = resolveS3Host(region, fips, dualStack);
                    if (s3Host != null) {
                        // The resolver answers with the bucket in the leading label; the endpoint setting names
                        // the service endpoint, which is what remains once that label is removed.
                        String serviceHost = s3Host.substring("mybucket.".length());
                        assertTrue(
                            "s3 endpoint rejected: " + serviceHost,
                            S3EndpointCheck.isPermittedHost(serviceHost, S3EndpointCheck.S3_SERVICE)
                        );
                        checked++;
                    }
                    String stsHost = resolveStsHost(region, fips, dualStack);
                    if (stsHost != null) {
                        assertTrue(
                            "sts endpoint rejected: " + stsHost,
                            S3EndpointCheck.isPermittedHost(stsHost, S3EndpointCheck.STS_SERVICE)
                        );
                        checked++;
                    }
                }
            }
        }
        // Guards against the loop silently resolving nothing and the assertions never running.
        assertTrue("expected the resolver to produce endpoints for every partition", checked >= 60);
    }

    public void testAcceptsGlobalAndVariantEndpoints() {
        assertPermitted("https://s3.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertPermitted("https://sts.amazonaws.com", S3EndpointCheck.STS_SERVICE);
        assertPermitted("https://s3-accesspoint.us-east-1.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertPermitted("https://s3express-use1-az4.us-east-1.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        // Case and the DNS root dot both normalise away.
        assertPermitted("https://S3.US-EAST-1.AMAZONAWS.COM", S3EndpointCheck.S3_SERVICE);
        assertPermitted("https://s3.us-east-1.amazonaws.com.", S3EndpointCheck.S3_SERVICE);
    }

    public void testAcceptsVpcInterfaceEndpoints() {
        // AWS PrivateLink: the destination a customer cannot express any other way. The SDK carries no
        // pattern for these, so these cases are what pins the spelling.
        assertPermitted("https://bucket.vpce-0a1b2c3d4e5f.s3.us-east-1.vpce.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertPermitted("https://accesspoint.vpce-0a1b2c3d.s3.eu-west-1.vpce.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertPermitted("https://control.vpce-0a1b2c3d.s3.cn-north-1.vpce.amazonaws.com.cn", S3EndpointCheck.S3_SERVICE);
        assertPermitted("https://vpce-0a1b2c3d.sts.us-east-1.vpce.amazonaws.com", S3EndpointCheck.STS_SERVICE);
    }

    public void testRefusesCustomerPublishedPrivateLinkService() {
        // A PrivateLink service a customer publishes themselves also lives under vpce.amazonaws.com, and its
        // backend is an NLB they control. The label after the endpoint id is vpce-svc-<id> rather than the
        // service name, which is the only thing separating it from an AWS-operated interface endpoint.
        assertRefused("https://vpce-0a1b.vpce-svc-0c2d.us-east-1.vpce.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertRefused("https://vpce-0a1b.vpce-svc-0c2d.us-east-1.vpce.amazonaws.com", S3EndpointCheck.STS_SERVICE);
        // An interface endpoint for a different AWS service is equally not ours.
        assertRefused("https://vpce-0a1b.ec2.us-east-1.vpce.amazonaws.com", S3EndpointCheck.S3_SERVICE);
    }

    public void testRefusesThirdPartyStores() {
        for (String host : List.of(
            "https://minio.example.com:9000",
            "https://play.min.io",
            "https://storage.googleapis.com",
            "https://s3.us-west-004.backblazeb2.com",
            "https://abc123.r2.cloudflarestorage.com",
            "https://ceph.internal:8080"
        )) {
            assertRefused(host, S3EndpointCheck.S3_SERVICE);
        }
    }

    public void testRefusesLinkLocalAndLiteralAddresses() {
        assertRefused("https://169.254.169.254", S3EndpointCheck.S3_SERVICE);
        assertRefused("https://127.0.0.1:9000", S3EndpointCheck.S3_SERVICE);
        assertRefused("https://[::ffff:169.254.169.254]", S3EndpointCheck.S3_SERVICE);
        assertRefused("https://[fd00::1]", S3EndpointCheck.S3_SERVICE);
    }

    public void testRefusesSpellingAttacks() {
        for (String value : List.of(
            // userinfo: everything before the @ is credentials, so the host is the attacker's
            "https://s3.us-east-1.amazonaws.com@evil.example.com/",
            "https://s3.us-east-1.amazonaws.com@@evil.example.com/",
            // an AWS-looking name extended by further labels
            "https://s3.us-east-1.amazonaws.com.evil.example.com",
            "https://s3.us-east-1.amazonaws.com.evil.example.com.",
            // fragment and query stuffing
            "https://evil.example.com#.s3.us-east-1.amazonaws.com",
            "https://evil.example.com?x=.s3.us-east-1.amazonaws.com",
            // percent-encoded and non-ASCII separators
            "https://s3.us-east-1.amazonaws.com%2e%2eevil.example.com",
            "https://s3.us-east-1.amazonaws.com。evil.example.com",
            "https://s3­.us-east-1.amazonaws.com",
            // punycode that merely looks like the real name
            "https://xn--s3-amazonaws.com",
            // empty labels
            "https://s3..us-east-1.amazonaws.com",
            "https://.s3.us-east-1.amazonaws.com",
            // backslash, which some parsers treat as a separator
            "https://s3.us-east-1.amazonaws.com\\@evil.example.com"
        )) {
            assertRefused(value, S3EndpointCheck.S3_SERVICE);
        }
    }

    public void testRefusesAwsHostsThatAreNotThisService() {
        // API Gateway gives an attacker a name under amazonaws.com whose content they control, which is why
        // the partition suffix alone cannot be the whole rule.
        assertRefused("https://abc123.execute-api.us-east-1.amazonaws.com", S3EndpointCheck.STS_SERVICE);
        assertRefused("https://abc123.execute-api.us-east-1.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        // A bucket anyone can create answers on a name whose leading label carries the sts- prefix. The
        // region label after the service label is what refuses it: that label is s3, not a region.
        assertRefused("https://sts-anything.s3.us-east-1.amazonaws.com", S3EndpointCheck.STS_SERVICE);
        assertRefused("https://sts.s3.us-east-1.amazonaws.com", S3EndpointCheck.STS_SERVICE);
        // Neither service accepts the other's endpoint.
        assertRefused("https://sts.us-east-1.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertRefused("https://s3.us-east-1.amazonaws.com", S3EndpointCheck.STS_SERVICE);
        // A bucket-qualified host is not a service endpoint; the SDK supplies the bucket label itself.
        assertRefused("https://mybucket.s3.us-east-1.amazonaws.com", S3EndpointCheck.S3_SERVICE);
    }

    public void testRefusesLabelWhereARegionIsRequired() {
        assertRefused("https://s3.notaregion.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertRefused("https://s3.us-east-1.evil.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        assertRefused("https://s3.dualstack.notaregion.amazonaws.com", S3EndpointCheck.S3_SERVICE);
        // A region belonging to another partition is still a region, so this is accepted; the destination
        // is AWS either way and the SDK, not this rule, decides whether the pairing resolves.
        assertPermitted("https://s3.cn-north-1.amazonaws.com", S3EndpointCheck.S3_SERVICE);
    }

    public void testValidateRequiresHttps() {
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(
            S3Configuration.fromMap(Map.of("endpoint", "http://s3.us-east-1.amazonaws.com", "auth", "anonymous")),
            errors
        );
        assertThat(errors.validationErrors().toString(), containsString("must use https"));
    }

    public void testValidateAcceptsAbsentEndpoints() {
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(S3Configuration.fromMap(Map.of("auth", "anonymous")), errors);
        assertTrue(errors.validationErrors().toString(), errors.validationErrors().isEmpty());
    }

    public void testAdditionalHostsPropertyName() {
        // Two test source sets repeat this string because neither can see S3EndpointCheck:
        // SeedingS3HttpFixture in this plugin's javaRestTest, and S3FixtureUtils in the esql qa modules.
        // Renaming the constant without updating both would leave every S3 integration suite unable to
        // reach its fixture, so the spelling is pinned here rather than left to a grep.
        assertEquals("org.elasticsearch.xpack.esql.datasource.s3.additionalEndpointHosts", S3EndpointCheck.ADDITIONAL_HOSTS_PROPERTY);
    }

    private static void assertPermitted(String url, String service) {
        assertTrue(url + " should be permitted for " + service, S3EndpointCheck.isPermittedHost(hostOf(url), service));
    }

    private static void assertRefused(String url, String service) {
        assertFalse(url + " should be refused for " + service, S3EndpointCheck.isPermittedHost(hostOf(url), service));
    }

    private static String hostOf(String url) {
        try {
            return java.net.URI.create(url).getHost();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String resolveS3Host(String region, boolean fips, boolean dualStack) {
        try {
            return S3EndpointProvider.defaultProvider()
                .resolveEndpoint(
                    S3EndpointParams.builder().region(Region.of(region)).bucket("mybucket").useFips(fips).useDualStack(dualStack).build()
                )
                .join()
                .url()
                .getHost();
        } catch (RuntimeException e) {
            // Not every partition offers every variant (aws-cn has no FIPS endpoints, the ISO
            // partitions no dual-stack ones). A combination the resolver refuses has no destination
            // for the rule to admit.
            return null;
        }
    }

    private static String resolveStsHost(String region, boolean fips, boolean dualStack) {
        try {
            return StsEndpointProvider.defaultProvider()
                .resolveEndpoint(StsEndpointParams.builder().region(Region.of(region)).useFips(fips).useDualStack(dualStack).build())
                .join()
                .url()
                .getHost();
        } catch (RuntimeException e) {
            return null;
        }
    }
}
