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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

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

    /** Every host the SDK resolver produces, in every partition, is accepted exactly when it names a region. */
    public void testAnswersForEveryEndpointTheResolverProduces() {
        Set<String> acceptedHosts = new TreeSet<>();
        Set<String> refusedHosts = new TreeSet<>();
        for (String region : REGIONS) {
            // The oracle is the resolver's own plain answer, not a spelling: STS spells dual-stack via api.aws.
            String plainS3 = resolveS3Host(region, false, false, false);
            String plainSts = resolveStsHost(region, false, false);
            assertNotNull("no plain S3 endpoint for " + region, plainS3);
            assertNotNull("no plain STS endpoint for " + region, plainSts);
            for (boolean fips : List.of(false, true)) {
                for (boolean dualStack : List.of(false, true)) {
                    for (boolean accelerate : List.of(false, true)) {
                        String s3Host = resolveS3Host(region, fips, dualStack, accelerate);
                        if (s3Host != null) {
                            // The resolver puts the bucket in the leading label; the setting names what remains.
                            String host = s3Host.substring("mybucket.".length());
                            if (s3Host.equals(plainS3)) {
                                assertTrue("rejected " + host, S3EndpointCheck.isPermittedHost(host, S3_SERVICE));
                                acceptedHosts.add(host);
                            } else {
                                assertFalse("accepted " + host, S3EndpointCheck.isPermittedHost(host, S3_SERVICE));
                                refusedHosts.add(host);
                            }
                        }
                    }
                    String stsHost = resolveStsHost(region, fips, dualStack);
                    if (stsHost != null) {
                        if (stsHost.equals(plainSts)) {
                            assertTrue("rejected " + stsHost, S3EndpointCheck.isPermittedHost(stsHost, STS_SERVICE));
                            acceptedHosts.add(stsHost);
                        } else {
                            assertFalse("accepted " + stsHost, S3EndpointCheck.isPermittedHost(stsHost, STS_SERVICE));
                            refusedHosts.add(stsHost);
                        }
                    }
                }
            }
        }
        // Counts distinct hosts, so this fails when a region drops out rather than when the SDK adds a combination.
        assertEquals("one permitted endpoint per service per region", 2 * REGIONS.size(), acceptedHosts.size());
        assertFalse("the sweep must exercise refusals too", refusedHosts.isEmpty());
        for (String host : acceptedHosts) {
            assertFalse("host both accepted and refused: " + host, refusedHosts.contains(host));
        }
    }

    /** Pseudo-regions are resolver input, never a region a host names. */
    public void testRefusesPseudoRegions() {
        assertAllRefused(
            S3_SERVICE,
            "s3.aws-global.amazonaws.com",
            "s3.aws-cn-global.amazonaws.com.cn",
            "s3.aws-us-gov-global.amazonaws.com",
            "s3.aws-iso-global.c2s.ic.gov",
            "s3-aws-global.amazonaws.com",
            "vpce-0a1b2c3d.s3.aws-global.vpce.amazonaws.com"
        );
        assertAllRefused(STS_SERVICE, "sts.aws-global.amazonaws.com", "sts.aws-us-gov-global.amazonaws.com");
    }

    /** A region is admitted because the SDK lists it, not because it looks like one. */
    public void testRefusesWellFormedButUnknownRegions() {
        assertAllRefused(
            S3_SERVICE,
            "s3.us-east-99.amazonaws.com",
            "s3.eu-west-9.amazonaws.com",
            "s3.xx-south-1.amazonaws.com",
            "s3-us-east-99.amazonaws.com",
            "vpce-0a1b2c3d.s3.us-east-99.vpce.amazonaws.com"
        );
        assertAllRefused(STS_SERVICE, "sts.us-east-99.amazonaws.com", "vpce-0a1b2c3d.sts.us-east-99.vpce.amazonaws.com");
    }

    /** Restates {@code S3EndpointCheck.S3_SERVICE_LABELS} independently, so enabling a label by accident turns this red. */
    public void testRefusesEveryDisabledFamilyInEveryRegion() {
        List<String> disabledS3 = List.of(
            "s3-fips",
            "s3-accesspoint",
            "s3-accesspoint-fips",
            "s3-accelerate",
            "s3-object-lambda",
            "s3-object-lambda-fips",
            "s3-outposts",
            "s3-outposts-fips",
            "s3-control",
            "s3-control-fips",
            "s3-external-1",
            "s3express-use1-az4",
            "s3express-control"
        );
        for (String region : REGIONS) {
            String prefix = "mybucket.s3." + region + ".";
            String suffix = resolveS3Host(region, false, false, false).substring(prefix.length());
            for (String label : disabledS3) {
                String host = label + "." + region + "." + suffix;
                assertFalse(host, S3EndpointCheck.isPermittedHost(host, S3_SERVICE));
            }
            String stsFips = "sts-fips." + region + "." + suffix;
            assertFalse(stsFips, S3EndpointCheck.isPermittedHost(stsFips, STS_SERVICE));
        }
    }

    public void testAcceptsRegionalEndpoints() {
        for (String host : List.of(
            "s3.us-east-1.amazonaws.com",
            "s3.eu-west-1.amazonaws.com",
            "s3.cn-north-1.amazonaws.com.cn",
            "s3.us-gov-west-1.amazonaws.com",
            // The historical spelling, which carries its region inside the service label.
            "s3-us-west-2.amazonaws.com",
            "S3.US-EAST-1.AMAZONAWS.COM",
            "s3.us-east-1.amazonaws.com."
        )) {
            assertTrue(host, S3EndpointCheck.isPermittedHost(host, S3_SERVICE));
        }
        for (String host : List.of("sts.us-east-1.amazonaws.com", "sts.eu-west-1.amazonaws.com", "STS.US-EAST-1.AMAZONAWS.COM")) {
            assertTrue(host, S3EndpointCheck.isPermittedHost(host, STS_SERVICE));
        }
    }

    /** They name no region, and pin to us-east-1 rather than following a cross-region redirect. */
    public void testAcceptsGlobalEndpoints() {
        assertTrue(S3EndpointCheck.isPermittedHost("s3.amazonaws.com", S3_SERVICE));
        assertTrue(S3EndpointCheck.isPermittedHost("sts.amazonaws.com", STS_SERVICE));
        assertTrue(S3EndpointCheck.isPermittedHost("S3.AMAZONAWS.COM", S3_SERVICE));
        assertAllRefused(S3_SERVICE, "sts.amazonaws.com", "s3-fips.amazonaws.com", "s3-accelerate.amazonaws.com");
        assertAllRefused(STS_SERVICE, "s3.amazonaws.com", "sts-fips.amazonaws.com");
    }

    /** The three prefixes are the ones AWS assigns for S3. */
    public void testAcceptsVpcInterfaceEndpoints() {
        for (String host : List.of(
            "bucket.vpce-0a1b2c3d4e5f.s3.us-east-1.vpce.amazonaws.com",
            "accesspoint.vpce-0a1b2c3d.s3.eu-west-1.vpce.amazonaws.com",
            "control.vpce-0a1b2c3d.s3.cn-north-1.vpce.amazonaws.com.cn",
            "vpce-0a1b2c3d.s3.us-east-1.vpce.amazonaws.com"
        )) {
            assertTrue(host, S3EndpointCheck.isPermittedHost(host, S3_SERVICE));
        }
        assertTrue(S3EndpointCheck.isPermittedHost("vpce-0a1b2c3d.sts.us-east-1.vpce.amazonaws.com", STS_SERVICE));
    }

    public void testRefusesPrefixedStsVpcInterfaceEndpoints() {
        assertAllRefused(
            STS_SERVICE,
            "bucket.vpce-0a1b2c3d.sts.us-east-1.vpce.amazonaws.com",
            "accesspoint.vpce-0a1b2c3d.sts.eu-west-1.vpce.amazonaws.com",
            "anything.vpce-0a1b2c3d.sts.us-east-1.vpce.amazonaws.com"
        );
    }

    /** The historical generator builds {@code s3-<region>}, so a region id {@code fips-…} would mint {@code s3-fips-…}. */
    public void testNoRegionIdCouldMintADisabledFamily() {
        for (Region region : Region.regions()) {
            if (region.isGlobalRegion()) {
                continue;
            }
            String id = region.id();
            for (String disabled : List.of("fips", "accesspoint", "accelerate", "object-lambda", "outposts", "control", "external")) {
                assertFalse(
                    "region id [" + id + "] would make the historical generator mint an s3-" + disabled + " host",
                    id.startsWith(disabled)
                );
            }
        }
    }

    /** Real AWS hosts, refused: an operator who needs one names it in the allowlist. */
    public void testRefusesNonRegionalAwsFamilies() {
        assertAllRefused(
            S3_SERVICE,
            "s3.dualstack.amazonaws.com",
            "s3-fips.amazonaws.com",
            "s3-accelerate.amazonaws.com",                // transfer acceleration
            "s3-accelerate.dualstack.amazonaws.com",
            "s3-accesspoint.us-east-1.amazonaws.com",     // access points
            "s3-accesspoint-fips.us-east-1.amazonaws.com",
            "s3-object-lambda.us-east-1.amazonaws.com",   // object lambda
            "s3-object-lambda-fips.us-east-1.amazonaws.com",
            "s3-outposts.us-east-1.amazonaws.com",        // Outposts
            "s3-outposts-fips.us-east-1.amazonaws.com",
            "s3-control.us-east-1.amazonaws.com",         // the account-level control plane
            "s3-control-fips.us-east-1.amazonaws.com",
            "s3-external-1.amazonaws.com",                // the legacy us-east-1 alias
            "s3-fips.us-east-1.amazonaws.com",            // FIPS, which an endpoint override cannot ask for
            "s3-fips.dualstack.eu-west-1.amazonaws.com"
        );
        assertAllRefused(STS_SERVICE, "sts.dualstack.amazonaws.com", "sts-fips.amazonaws.com", "sts-fips.us-east-1.amazonaws.com");
    }

    /** {@link S3ResourceCheck} refuses directory buckets by name, so the endpoint that serves them is refused too. */
    public void testRefusesS3ExpressEndpoints() {
        assertAllRefused(S3_SERVICE, "s3express-use1-az4.us-east-1.amazonaws.com", "s3express-control.us-east-1.amazonaws.com");
    }

    /** Refused because nothing here is tested over dual-stack, not because it cannot be reached. */
    public void testRefusesDualStackEndpoints() {
        assertAllRefused(
            S3_SERVICE,
            "s3.dualstack.us-east-1.amazonaws.com",
            "s3.dualstack.amazonaws.com",
            "mybucket.s3.dualstack.us-east-1.amazonaws.com",
            "dualstack.s3.us-east-1.amazonaws.com"
        );
        assertAllRefused(STS_SERVICE, "sts.dualstack.us-east-1.amazonaws.com");
    }

    public void testAcceptsTheHistoricalDashRegionSpelling() {
        assertTrue(S3EndpointCheck.isPermittedHost("s3-eu-west-1.amazonaws.com", S3_SERVICE));
        assertAllRefused(S3_SERVICE, "s3-notaregion.amazonaws.com", "s3-.amazonaws.com");
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

    /** Driven through URI parsing: for several of these the defence is what {@code getHost()} returns. */
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

    /** Names an attacker can obtain under an AWS suffix; the region after the service label is what refuses them. */
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
            "mybucket.s3.us-east-1.amazonaws.com",
            "s3.notaregion.amazonaws.com",
            "s3.us-east-1.evil.amazonaws.com",
            "s3.dualstack.notaregion.amazonaws.com"
        );
    }

    /** Without the leading dot, {@code eu-west-1xamazonaws.com} — a registrable domain — would pass. */
    public void testRefusesSuffixWithoutALabelBoundary() {
        assertAllRefused(S3_SERVICE, "s3.eu-west-1xamazonaws.com", "s3.us-east-1xapi.aws", "s3.us-east-11amazonaws.com");
        assertAllRefused(STS_SERVICE, "sts.eu-west-1xamazonaws.com");
    }

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
            // Each case below is the only one that pins its check; see S3EndpointCheck.isVpcInterfaceEndpoint.
            "evil.s3.us-east-1.vpce.amazonaws.com",
            "s3.s3.us-east-1.vpce.amazonaws.com",
            "a.b.vpce-0a1b.s3.us-east-1.vpce.amazonaws.com",
            "vpce-0a1b.s3.us-east-1.notvpce.amazonaws.com",
            "vpce-0a1b.s3.us-east-1.vpce.amazonaws.com.attacker.example.com",
            "vpce-0a1b.s3.us-east-1.vpce.amazonaws.com.evil.co",
            "vpce-0a1b.us-east-1.vpce.amazonaws.com",
            "vpce-0a1b.s3.notaregion.vpce.amazonaws.com"
        );
    }

    public void testValidateAcceptsAPermittedHostCarryingAPort() {
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(config("https://s3.us-east-1.amazonaws.com:9000"), ALLOW_NOTHING, errors);
        assertTrue(errors.validationErrors().toString(), errors.validationErrors().isEmpty());
    }

    public void testValidateRequiresHttps() {
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(config("http://s3.us-east-1.amazonaws.com"), ALLOW_NOTHING, errors);
        assertThat(errors.validationErrors().toString(), containsString("must use https"));
    }

    public void testValidateAcceptsAbsentEndpoints() {
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(S3Configuration.fromMap(Map.of("auth", "anonymous")), ALLOW_NOTHING, errors);
        assertTrue(errors.validationErrors().toString(), errors.validationErrors().isEmpty());
    }

    private static String hostOf(String url) {
        try {
            return URI.create(url).getHost();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public void testStsEndpointRequiresHttpsEvenWhenAllowlisted() {
        Predicate<String> allowLoopback = hostAndPort -> hostAndPort.startsWith("127.0.0.1:");
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(stsConfig("http://127.0.0.1:9000"), allowLoopback, errors);
        assertThat(errors.getMessage(), containsString("sts_endpoint [http://127.0.0.1:9000] must use https"));

        ValidationException overHttps = new ValidationException();
        S3EndpointCheck.validate(stsConfig("https://127.0.0.1:9000"), allowLoopback, overHttps);
        assertTrue(overHttps.validationErrors().isEmpty());
    }

    /** An entry waives the AWS-host rule; a host it does not name stays refused. */
    public void testOperatorAllowlistAdmitsExactlyWhatItNames() {
        Predicate<String> loopback = hostAndPort -> hostAndPort.startsWith("127.0.0.1:");
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(config("http://127.0.0.1:9000"), loopback, errors);
        assertTrue(errors.validationErrors().toString(), errors.validationErrors().isEmpty());

        errors = new ValidationException();
        S3EndpointCheck.validate(config("http://10.0.0.1:9000"), loopback, errors);
        assertThat(errors.validationErrors().toString(), containsString("must use https"));

        errors = new ValidationException();
        S3EndpointCheck.validate(config("http://127.0.0.1:9000"), ALLOW_NOTHING, errors);
        assertThat(errors.validationErrors().toString(), containsString("must use https"));
    }

    private static final Predicate<String> ALLOW_NOTHING = hostAndPort -> false;

    private static S3Configuration stsConfig(String stsEndpoint) {
        return S3Configuration.fromMap(Map.of("role_arn", "arn:aws:iam::123456789012:role/example", "sts_endpoint", stsEndpoint));
    }

    private static S3Configuration config(String endpoint) {
        return S3Configuration.fromMap(Map.of("endpoint", endpoint, "auth", "anonymous"));
    }

    private static void assertAllRefused(String service, String... hosts) {
        for (String host : hosts) {
            assertFalse(host + " for " + service, S3EndpointCheck.isPermittedHost(host, service));
        }
    }

    private static String resolveS3Host(String region, boolean fips, boolean dualStack, boolean accelerate) {
        try {
            var params = S3EndpointParams.builder().region(Region.of(region)).bucket("mybucket");
            return S3EndpointProvider.defaultProvider()
                .resolveEndpoint(params.useFips(fips).useDualStack(dualStack).accelerate(accelerate).build())
                .join()
                .url()
                .getHost();
        } catch (RuntimeException e) {
            // aws-cn has no FIPS endpoints, the ISO partitions no dual-stack ones, and accelerate is not
            // served with FIPS; a combination the resolver refuses has no destination for the rule to admit.
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
