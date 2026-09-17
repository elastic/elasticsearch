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

import org.elasticsearch.Build;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasource.s3.S3EndpointCheck.S3_SERVICE;
import static org.elasticsearch.xpack.esql.datasource.s3.S3EndpointCheck.STS_SERVICE;
import static org.hamcrest.Matchers.containsString;

public class S3EndpointCheckTests extends ESTestCase {

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

    /**
     * Asks the SDK's own resolver where each region, FIPS and dual-stack combination in all eight AWS
     * partitions resolves to, and requires the rule to accept every one. A rule written against a single
     * literal suffix cannot pass, and a partition the SDK gains is covered without editing this file.
     */
    public void testAcceptsEveryEndpointTheResolverProduces() {
        int checked = 0;
        for (String region : REGIONS) {
            for (boolean fips : List.of(false, true)) {
                for (boolean dualStack : List.of(false, true)) {
                    String s3Host = resolveS3Host(region, fips, dualStack);
                    if (s3Host != null) {
                        // The resolver puts the bucket in the leading label; the setting names what remains.
                        String service = s3Host.substring("mybucket.".length());
                        assertTrue("rejected " + service, S3EndpointCheck.isPermittedHost(service, S3_SERVICE));
                        checked++;
                    }
                    String stsHost = resolveStsHost(region, fips, dualStack);
                    if (stsHost != null) {
                        assertTrue("rejected " + stsHost, S3EndpointCheck.isPermittedHost(stsHost, STS_SERVICE));
                        checked++;
                    }
                }
            }
        }
        assertEquals("the set of endpoints the resolver produces has changed", 68, checked);
    }

    public void testAcceptsAwsEndpoints() {
        for (String host : List.of(
            "s3.amazonaws.com",
            "s3-accesspoint.us-east-1.amazonaws.com",
            "s3-accesspoint-fips.us-east-1.amazonaws.com",
            "s3-object-lambda.us-east-1.amazonaws.com",
            "s3-outposts.us-east-1.amazonaws.com",
            "s3-control.us-east-1.amazonaws.com",
            "s3-accelerate.amazonaws.com",
            "s3-external-1.amazonaws.com",
            "s3-us-west-2.amazonaws.com",
            "S3.US-EAST-1.AMAZONAWS.COM",
            "s3.us-east-1.amazonaws.com.",
            // AWS PrivateLink, in the three prefixes AWS assigns for S3.
            "bucket.vpce-0a1b2c3d4e5f.s3.us-east-1.vpce.amazonaws.com",
            "accesspoint.vpce-0a1b2c3d.s3.eu-west-1.vpce.amazonaws.com",
            "control.vpce-0a1b2c3d.s3.cn-north-1.vpce.amazonaws.com.cn"
        )) {
            assertTrue(host, S3EndpointCheck.isPermittedHost(host, S3_SERVICE));
        }
        for (String host : List.of("sts.amazonaws.com", "vpce-0a1b2c3d.sts.us-east-1.vpce.amazonaws.com")) {
            assertTrue(host, S3EndpointCheck.isPermittedHost(host, STS_SERVICE));
        }
    }

    /**
     * An S3 Express endpoint serves only directory buckets, and those are refused by name in
     * {@link S3ResourceCheck}, so accepting the endpoint that reaches them would be incoherent.
     */
    public void testRefusesS3ExpressEndpoints() {
        assertAllRefused(S3_SERVICE, "s3express-use1-az4.us-east-1.amazonaws.com", "s3express-control.us-east-1.amazonaws.com");
    }

    /** Dual-stack is always regional: the resolver produces no region-less dual-stack endpoint. */
    public void testRefusesDualStackWithoutARegion() {
        assertAllRefused(S3_SERVICE, "s3.dualstack.amazonaws.com", "s3-fips.dualstack.amazonaws.com");
        assertAllRefused(STS_SERVICE, "sts.dualstack.amazonaws.com");
    }

    public void testRefusesNonAwsHosts() {
        assertAllRefused(
            S3_SERVICE,
            "minio.example.com",
            "play.min.io",
            "storage.googleapis.com",
            "s3.us-west-004.backblazeb2.com",
            "abc123.r2.cloudflarestorage.com",
            "169.254.169.254",
            "[fd00::1]"
        );
    }

    /**
     * The spelling attacks, all defeated by taking the host from {@link URI#getHost()} rather than the
     * authority and anchoring every suffix test with a leading dot. Driven through URI parsing on purpose:
     * for several of these the defence is that {@code getHost()} returns something other than what the
     * string appears to say.
     */
    public void testRefusesSpellingAttacks() {
        for (String url : List.of(
            "https://s3.us-east-1.amazonaws.com@evil.example.com/",
            "https://s3.us-east-1.amazonaws.com@@evil.example.com/",
            "https://s3.us-east-1.amazonaws.com.evil.example.com",
            "https://evil.example.com#.s3.us-east-1.amazonaws.com",
            "https://evil.example.com?x=.s3.us-east-1.amazonaws.com",
            "https://s3.us-east-1.amazonaws.com%2e%2eevil.example.com",
            "https://s3.us-east-1.amazonaws.com。evil.example.com",
            "https://s3­.us-east-1.amazonaws.com",
            "https://xn--s3-amazonaws.com",
            "https://s3..us-east-1.amazonaws.com",
            "https://.s3.us-east-1.amazonaws.com",
            "https://s3.us-east-1.amazonaws.com\\@evil.example.com",
            "https://[::ffff:169.254.169.254]"
        )) {
            assertFalse(url, S3EndpointCheck.isPermittedHost(hostOf(url), S3_SERVICE));
        }
    }

    /**
     * Hosts under an AWS partition suffix that are not this service's endpoint. {@code execute-api} and a
     * bucket named {@code sts-anything} are both names an attacker can obtain, which is why the partition
     * suffix alone cannot be the rule: what refuses them is the region required after the service label.
     */
    public void testRefusesAwsHostsThatAreNotThisService() {
        assertAllRefused(
            STS_SERVICE,
            "abc123.execute-api.us-east-1.amazonaws.com",
            "sts-anything.s3.us-east-1.amazonaws.com",
            "sts.s3.us-east-1.amazonaws.com",
            "sts-evil.us-east-1.amazonaws.com",
            "s3.us-east-1.amazonaws.com"
        );
        assertAllRefused(
            S3_SERVICE,
            "sts.us-east-1.amazonaws.com",
            "s3-evil.us-east-1.amazonaws.com",
            // A bucket-qualified host: the SDK supplies the bucket label itself.
            "mybucket.s3.us-east-1.amazonaws.com",
            "s3.notaregion.amazonaws.com",
            "s3.us-east-1.evil.amazonaws.com",
            "s3.dualstack.notaregion.amazonaws.com"
        );
    }

    /**
     * Without the leading dot, {@code s3.eu-west-1xamazonaws.com} passes — and
     * {@code eu-west-1xamazonaws.com} is an ordinary registrable domain. The suffix-extension cases above
     * do not cover this: the label arithmetic refuses those for a second, independent reason.
     */
    public void testRefusesSuffixWithoutALabelBoundary() {
        assertAllRefused(S3_SERVICE, "s3.eu-west-1xamazonaws.com", "s3.us-east-1xapi.aws", "s3.us-east-11amazonaws.com");
        assertAllRefused(STS_SERVICE, "sts.eu-west-1xamazonaws.com");
    }

    /**
     * The interface-endpoint shape has every part in a fixed position. An unpositioned scan for a
     * {@code (vpce-*, service)} pair admits all of these — including a customer-published PrivateLink
     * service, whose {@code vpce-svc-<id>} label satisfies the {@code vpce-} test on its own.
     */
    public void testRefusesVpcFormsOutsideTheExactShape() {
        assertAllRefused(
            STS_SERVICE,
            "vpce-0a1b.sts.anything.at.all.vpce.amazonaws.com",
            "vpce-0a1b.sts.vpce.amazonaws.com",
            "evil.vpce-0a1b.sts.evil.vpce.amazonaws.com"
        );
        assertAllRefused(
            S3_SERVICE,
            "vpce-0a1b.vpce-svc-0c2d.us-east-1.vpce.amazonaws.com",
            "vpce-svc-0c2d.s3.us-east-1.vpce.amazonaws.com",
            "vpce-0a1b.ec2.us-east-1.vpce.amazonaws.com",
            "a.b.vpce-0a1b.s3.us-east-1.vpce.amazonaws.com",
            "vpce-0a1b.s3.us-east-1.notvpce.amazonaws.com",
            "vpce-0a1b.us-east-1.vpce.amazonaws.com"
        );
    }

    public void testValidateRequiresHttps() {
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(config("http://s3.us-east-1.amazonaws.com"), errors);
        assertThat(errors.validationErrors().toString(), containsString("must use https"));
    }

    /**
     * The fixture allowance, which is the one branch that admits a non-AWS host. Driven through
     * {@link S3EndpointCheck#validate} rather than {@code isPermittedHost}, because that is the only entry
     * point that consults it. Both arms run on a snapshot build, which is what the unit suite is.
     */
    public void testAcceptsLoopbackFixtureHostsOnlyOnASnapshotBuild() {
        assertTrue(Build.current().isSnapshot());
        for (String endpoint : List.of("http://127.0.0.1:9000", "http://[::1]:9000", "http://localhost:9000")) {
            ValidationException errors = new ValidationException();
            S3EndpointCheck.validate(config(endpoint), errors);
            assertTrue(endpoint + " -> " + errors.validationErrors(), errors.validationErrors().isEmpty());
        }
        // A private address is not loopback, so it stays refused even here.
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(config("http://10.0.0.1:9000"), errors);
        assertThat(errors.validationErrors().toString(), containsString("must use https"));
    }

    public void testValidateAcceptsAbsentEndpoints() {
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(S3Configuration.fromMap(Map.of("auth", "anonymous")), errors);
        assertTrue(errors.validationErrors().toString(), errors.validationErrors().isEmpty());
    }

    /** The host as the validator sees it: null when the value does not parse, which is itself a refusal. */
    private static String hostOf(String url) {
        try {
            return URI.create(url).getHost();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static S3Configuration config(String endpoint) {
        return S3Configuration.fromMap(Map.of("endpoint", endpoint, "auth", "anonymous"));
    }

    private static void assertAllRefused(String service, String... hosts) {
        for (String host : hosts) {
            assertFalse(host + " for " + service, S3EndpointCheck.isPermittedHost(host, service));
        }
    }

    private static String resolveS3Host(String region, boolean fips, boolean dualStack) {
        try {
            var params = S3EndpointParams.builder().region(Region.of(region)).bucket("mybucket");
            return S3EndpointProvider.defaultProvider()
                .resolveEndpoint(params.useFips(fips).useDualStack(dualStack).build())
                .join()
                .url()
                .getHost();
        } catch (RuntimeException e) {
            // aws-cn has no FIPS endpoints and the ISO partitions no dual-stack ones; a combination the
            // resolver refuses has no destination for the rule to admit.
            return null;
        }
    }

    private static String resolveStsHost(String region, boolean fips, boolean dualStack) {
        try {
            var params = StsEndpointParams.builder().region(Region.of(region));
            return StsEndpointProvider.defaultProvider()
                .resolveEndpoint(params.useFips(fips).useDualStack(dualStack).build())
                .join()
                .url()
                .getHost();
        } catch (RuntimeException e) {
            return null;
        }
    }
}
