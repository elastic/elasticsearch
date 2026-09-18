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

    /**
     * Asks the SDK's own resolver where each FIPS, dual-stack and transfer-acceleration combination resolves
     * to, over one region per partition plus three more in {@code aws}, and requires the rule to answer for
     * each destination the way the permitted set says it should: accept it when the resolver named a region
     * in the host, refuse it when it did not. Every partition is covered; a further region inside one adds no
     * host shape the rule treats differently. A rule written against a single literal suffix cannot pass.
     *
     * <p>Both directions come from the resolver rather than from a list written here, which is what makes the
     * refusals worth asserting: transfer acceleration is the axis that produces a region-less host
     * ({@code s3-accelerate.dualstack.amazonaws.com}), and a sweep that only ever asked for acceptance would
     * pass whether or not the rule still refused it.
     *
     * <p>The population is the real regions above. The pseudo-regions {@link Region#regions()} also carries
     * ({@code aws-us-gov-global} and the per-partition {@code aws-*-global} names) are deliberately outside
     * it: they are an input to the resolver rather than an endpoint anyone configures.
     */
    public void testAnswersForEveryEndpointTheResolverProduces() {
        int accepted = 0;
        int refused = 0;
        for (String region : REGIONS) {
            for (boolean fips : List.of(false, true)) {
                for (boolean dualStack : List.of(false, true)) {
                    for (boolean accelerate : List.of(false, true)) {
                        String s3Host = resolveS3Host(region, fips, dualStack, accelerate);
                        if (s3Host != null) {
                            // The resolver puts the bucket in the leading label; the setting names what remains.
                            String service = s3Host.substring("mybucket.".length());
                            if (shouldBeAccepted(service, region)) {
                                assertTrue("rejected " + service, S3EndpointCheck.isPermittedHost(service, S3_SERVICE));
                                accepted++;
                            } else {
                                assertFalse("accepted " + service, S3EndpointCheck.isPermittedHost(service, S3_SERVICE));
                                refused++;
                            }
                        }
                    }
                    String stsHost = resolveStsHost(region, fips, dualStack);
                    if (stsHost != null) {
                        if (shouldBeAccepted(stsHost, region)) {
                            assertTrue("rejected " + stsHost, S3EndpointCheck.isPermittedHost(stsHost, STS_SERVICE));
                            accepted++;
                        } else {
                            assertFalse("accepted " + stsHost, S3EndpointCheck.isPermittedHost(stsHost, STS_SERVICE));
                            refused++;
                        }
                    }
                }
            }
        }
        assertEquals("the set of endpoints the resolver produces has changed", 36, accepted);
        assertEquals("the set of region-less endpoints the resolver produces has changed", 50, refused);
    }

    /**
     * Whether the rule should accept this destination: the resolver put the region into the host, as a label
     * of its own or inside the service label, and the host is not a FIPS endpoint. FIPS is refused even
     * though it names a region — see {@code S3EndpointCheck.S3_SERVICE_LABELS} for why.
     */
    private static boolean shouldBeAccepted(String host, String region) {
        if (host.startsWith("s3-fips.") || host.startsWith("sts-fips.")) {
            return false;
        }
        return host.contains("." + region + ".") || host.startsWith("s3-" + region + ".");
    }

    public void testAcceptsAwsEndpoints() {
        for (String host : List.of(
            "s3.us-east-1.amazonaws.com",
            "s3.dualstack.eu-west-1.amazonaws.com",
            "s3.cn-north-1.amazonaws.com.cn",
            // The historical spelling, which carries its region inside the service label.
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
        for (String host : List.of("sts.us-east-1.amazonaws.com", "vpce-0a1b2c3d.sts.us-east-1.vpce.amazonaws.com")) {
            assertTrue(host, S3EndpointCheck.isPermittedHost(host, STS_SERVICE));
        }
    }

    /**
     * Every AWS endpoint family outside the regional object endpoint and PrivateLink. Each of these is a real
     * AWS host, under an AWS suffix, and each is refused: an operator who needs one names it in
     * {@code esql.external.allowed_endpoint_hosts} rather than having it permitted for everybody.
     *
     * <p>The global endpoint is in here rather than in the regional set on purpose. {@code s3.amazonaws.com}
     * is the most commonly typed override there is, and it names no region — it answers for
     * {@code us-east-1} and redirects elsewhere, which is exactly the destination-follows-the-request
     * behaviour a host rule cannot confine.
     */
    public void testRefusesNonRegionalAwsFamilies() {
        assertAllRefused(
            S3_SERVICE,
            "s3.amazonaws.com",                           // the global endpoint
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
        assertAllRefused(
            STS_SERVICE,
            "sts.amazonaws.com",
            "sts.dualstack.amazonaws.com",
            "sts-fips.amazonaws.com",
            "sts-fips.us-east-1.amazonaws.com"
        );
    }

    /**
     * An S3 Express endpoint serves only directory buckets, and those are refused by name in
     * {@link S3ResourceCheck}, so accepting the endpoint that reaches them would be incoherent.
     */
    public void testRefusesS3ExpressEndpoints() {
        assertAllRefused(S3_SERVICE, "s3express-use1-az4.us-east-1.amazonaws.com", "s3express-control.us-east-1.amazonaws.com");
    }

    /**
     * Dual-stack is a modifier on a regional endpoint, never a substitute for the region. A bucket-qualified
     * name is refused for the same reason the global form is: {@code dualstack} is a fixed label compared in
     * second position, so the label the customer chose never lands where a region is required.
     */
    public void testDualStackNeedsARegion() {
        assertTrue(S3EndpointCheck.isPermittedHost("s3.dualstack.us-east-1.amazonaws.com", S3_SERVICE));
        assertAllRefused(
            S3_SERVICE,
            "s3.dualstack.amazonaws.com",
            "mybucket.s3.dualstack.us-east-1.amazonaws.com",
            "dualstack.s3.us-east-1.amazonaws.com"
        );
    }

    /**
     * The historical spelling is the one accepted form with no region label of its own, because the region is
     * inside the service label. A leading {@code s3-} that is not followed by a region does not qualify.
     */
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
            "vpce-0a1b.us-east-1.vpce.amazonaws.com",
            // The region requirement on the S3 arm. Every case above is refused for a second reason as
            // well, so without this one the requirement is pinned only by its STS counterpart.
            "vpce-0a1b.s3.notaregion.vpce.amazonaws.com"
        );
    }

    /** A port does not change the host: {@link java.net.URI#getHost()} excludes it, and the docs allow one. */
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

    /** The host as the validator sees it: null when the value does not parse, which is itself a refusal. */
    private static String hostOf(String url) {
        try {
            return URI.create(url).getHost();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * The operator allowlist, which is the only route to a host that is not an AWS endpoint. An entry waives
     * the AWS-host rule and the https requirement together, because the node's own configuration named it.
     */
    public void testOperatorAllowlistAdmitsExactlyWhatItNames() {
        Predicate<String> loopback = hostAndPort -> hostAndPort.startsWith("127.0.0.1:");
        ValidationException errors = new ValidationException();
        S3EndpointCheck.validate(config("http://127.0.0.1:9000"), loopback, errors);
        assertTrue(errors.validationErrors().toString(), errors.validationErrors().isEmpty());

        // A host the allowlist does not name stays refused, allowlist or no allowlist.
        errors = new ValidationException();
        S3EndpointCheck.validate(config("http://10.0.0.1:9000"), loopback, errors);
        assertThat(errors.validationErrors().toString(), containsString("must use https"));

        // And with the default empty allowlist the same loopback value is refused.
        errors = new ValidationException();
        S3EndpointCheck.validate(config("http://127.0.0.1:9000"), ALLOW_NOTHING, errors);
        assertThat(errors.validationErrors().toString(), containsString("must use https"));
    }

    /** The default posture: the list is the enable, and it is empty unless an operator sets it. */
    private static final Predicate<String> ALLOW_NOTHING = hostAndPort -> false;

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
