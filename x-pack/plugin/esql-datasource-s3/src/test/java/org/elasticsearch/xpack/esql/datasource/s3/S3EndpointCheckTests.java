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
        Set<String> acceptedHosts = new TreeSet<>();
        Set<String> refusedHosts = new TreeSet<>();
        for (String region : REGIONS) {
            // The destination the resolver picks with every option off is the one the rule permits. Stating
            // the oracle as "the resolver's own plain answer" rather than as a host spelling is what keeps
            // it independent of the rule: STS spells dual-stack by changing the suffix to api.aws, not by
            // inserting a label, so an oracle written against the region's position would accept it.
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
        // What gates the rule is the assertTrue/assertFalse above, on every destination; these assertions
        // gate the sweep itself, which is a different job. Counting distinct hosts rather than resolver
        // combinations is what makes that stable: the plain S3 and STS endpoints are one each per region
        // however many FIPS, dual-stack and acceleration combinations the resolver answers for it, so this
        // fails when the sweep stops covering a region rather than when the SDK gains a combination. The
        // combination counts it replaces moved on every SDK bump, and a moved count gets re-pasted.
        assertEquals("one permitted endpoint per service per region", 2 * REGIONS.size(), acceptedHosts.size());
        assertFalse("the sweep must exercise refusals too", refusedHosts.isEmpty());
        for (String host : acceptedHosts) {
            assertFalse("host both accepted and refused: " + host, refusedHosts.contains(host));
        }
    }

    /**
     * The pseudo-regions {@link Region#regions()} carries alongside the real ones. They are an input to the
     * SDK's resolver — {@code aws-global} selects the region-less global endpoint — and never a region a
     * host names, so none of them may reach the permitted set as though it were one.
     */
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

    /**
     * A region token that is well formed and names nothing. The rule admits a region because the pinned SDK
     * lists it, never because it looks like one, so these are refused in every accepted shape — regional,
     * historical and PrivateLink alike. A real new AWS region is readmitted by upgrading the SDK.
     */
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

    /**
     * Every endpoint family this data source does not support, refused across every partition, so that the
     * family itself is what refuses them rather than an unrelated region or suffix. The list restates
     * {@code S3EndpointCheck.S3_SERVICE_LABELS} independently: enabling a label there without meaning to
     * turns this red, which is why the disabled families stay enumerated rather than forgotten.
     */
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

    /**
     * The two global endpoints. They name no region, so they reach a {@code us-east-1} bucket and fail for
     * any other — setting any endpoint turns off the SDK's cross-region decorator, so the redirect S3
     * answers with is not followed. They are admitted because they are AWS hosts that work, and refusing the
     * most commonly typed override of all would break configurations for no gain.
     *
     * <p>Only the bare service label has a global form. The per-partition pseudo-region spellings stay
     * refused — see {@link #testRefusesPseudoRegions}.
     */
    public void testAcceptsGlobalEndpoints() {
        assertTrue(S3EndpointCheck.isPermittedHost("s3.amazonaws.com", S3_SERVICE));
        assertTrue(S3EndpointCheck.isPermittedHost("sts.amazonaws.com", STS_SERVICE));
        assertTrue(S3EndpointCheck.isPermittedHost("S3.AMAZONAWS.COM", S3_SERVICE));
        // The global form belongs to the bare service only, and not to the other service.
        assertAllRefused(S3_SERVICE, "sts.amazonaws.com", "s3-fips.amazonaws.com", "s3-accelerate.amazonaws.com");
        assertAllRefused(STS_SERVICE, "s3.amazonaws.com", "sts-fips.amazonaws.com");
    }

    /**
     * AWS PrivateLink, which is how a cluster with no route to the public internet reaches S3, and therefore
     * the form that has to keep working. The three prefixes are the ones AWS assigns for S3; STS publishes
     * the same shape with no prefix. {@link #testRefusesVpcFormsOutsideTheExactShape} is the other half.
     */
    public void testAcceptsVpcInterfaceEndpoints() {
        for (String host : List.of(
            "bucket.vpce-0a1b2c3d4e5f.s3.us-east-1.vpce.amazonaws.com",
            "accesspoint.vpce-0a1b2c3d.s3.eu-west-1.vpce.amazonaws.com",
            "control.vpce-0a1b2c3d.s3.cn-north-1.vpce.amazonaws.com.cn",
            // No prefix at all, which is the shape the id-only form takes.
            "vpce-0a1b2c3d.s3.us-east-1.vpce.amazonaws.com"
        )) {
            assertTrue(host, S3EndpointCheck.isPermittedHost(host, S3_SERVICE));
        }
        assertTrue(S3EndpointCheck.isPermittedHost("vpce-0a1b2c3d.sts.us-east-1.vpce.amazonaws.com", STS_SERVICE));
    }

    /**
     * Every AWS endpoint family outside the regional object endpoint and PrivateLink. Each of these is a real
     * AWS host, under an AWS suffix, and each is refused: an operator who needs one names it in
     * {@code esql.external.allowed_endpoint_hosts} rather than having it permitted for everybody.
     *
     */
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

    /**
     * An S3 Express endpoint serves only directory buckets, and those are refused by name in
     * {@link S3ResourceCheck}, so accepting the endpoint that reaches them would be incoherent.
     */
    public void testRefusesS3ExpressEndpoints() {
        assertAllRefused(S3_SERVICE, "s3express-use1-az4.us-east-1.amazonaws.com", "s3express-control.us-east-1.amazonaws.com");
    }

    /**
     * Dual-stack, refused in every spelling including the regional one. An endpoint override cannot ask AWS
     * for dual-stack — the SDK rejects its dual-stack client option when one is set, and this data source
     * exposes no such option — so the host would promise an IPv6 path nothing here arranges.
     */
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
            // The id position must hold an endpoint id, not merely one label. Without this the shape
            // accepts any single label AWS has not minted a name for.
            "evil.s3.us-east-1.vpce.amazonaws.com",
            "s3.s3.us-east-1.vpce.amazonaws.com",
            "a.b.vpce-0a1b.s3.us-east-1.vpce.amazonaws.com",
            "vpce-0a1b.s3.us-east-1.notvpce.amazonaws.com",
            // The tail must sit at the end of the host. Today the head arithmetic refuses this for a
            // second reason — the tail begins with a dot, so the label before it can never start vpce- —
            // which means without this case the anchoring itself is pinned by nothing.
            "vpce-0a1b.s3.us-east-1.vpce.amazonaws.com.attacker.example.com",
            "vpce-0a1b.s3.us-east-1.vpce.amazonaws.com.evil.co",
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
