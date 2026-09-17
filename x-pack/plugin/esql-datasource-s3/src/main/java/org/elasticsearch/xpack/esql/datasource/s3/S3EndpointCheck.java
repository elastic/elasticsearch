/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.regions.EndpointTag;
import software.amazon.awssdk.regions.PartitionEndpointKey;
import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;

import org.elasticsearch.Build;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.SuppressForbidden;

import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Provider-specific validation for the {@code endpoint} and {@code sts_endpoint} data-source settings,
 * invoked at {@code PUT /_query/data_source} time via
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withDatasourceCheck}.
 *
 * <p>Both settings become an endpoint override on an AWS SDK client builder, so whatever host they name
 * is a host this node connects to — for {@code sts_endpoint}, one that receives the node's own OIDC token
 * as a bearer credential, because that client authenticates with nothing else. Neither setting is
 * constrained by anything upstream: the shared URL check confirms the value is an absolute http or https
 * URL with a parseable host and makes no judgement about which host.
 *
 * <p>This class confines both to the AWS endpoints the product supports. A value is permitted when its
 * host takes one of two forms, under one of the AWS partition DNS suffixes:
 *
 * <ul>
 *   <li><b>Service endpoint</b> — {@code <service>[.dualstack].<region>.<suffix>}, or the global
 *       {@code <service>.<suffix>}. The leading label carries the service and any variant the resolver
 *       spells there ({@code s3}, {@code s3-fips}, {@code s3-accesspoint}, {@code s3express-<az>},
 *       {@code sts}, {@code sts-fips}).</li>
 *   <li><b>VPC interface endpoint</b> — {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce.<suffix>},
 *       the AWS PrivateLink form. This is the one destination a customer cannot reach any other way,
 *       so refusing it would take away a supported deployment.</li>
 * </ul>
 *
 * <p>Requiring a region label after the service label is load-bearing rather than tidiness. Without it
 * any customer-controlled name whose leading label happens to start with the service prefix is admitted:
 * an S3 bucket called {@code sts-anything} answers to
 * {@code sts-anything.s3.us-east-1.amazonaws.com}, which carries the {@code sts-} prefix and the AWS
 * suffix, and would otherwise be a legal destination for the node's token. The region label breaks that,
 * because the label following the service is then {@code s3} rather than a region.
 *
 * <p>The suffixes and the region patterns are read out of the SDK's own partition metadata rather than
 * written down here, so a partition or region the SDK gains is covered without a code change. The
 * interface-endpoint half has no such source — {@code vpce} appears in none of the SDK jars — so that
 * pattern is maintained here and pinned by tests.
 *
 * <p>The host is taken from {@link URI#getHost()}, never {@code getAuthority()}, and every suffix
 * comparison is anchored with a leading dot. That combination is what defeats the spelling attacks:
 * userinfo before an AWS-looking name ({@code https://s3.us-east-1.amazonaws.com@evil.example.com}) has
 * host {@code evil.example.com}, an AWS-looking name extended by a further label
 * ({@code s3.us-east-1.amazonaws.com.evil.example.com}) fails the anchored suffix test, and
 * percent-encoded or non-ASCII separators leave {@code getHost()} null.
 */
final class S3EndpointCheck {

    static final String S3_SERVICE = "s3";
    static final String STS_SERVICE = "sts";

    /**
     * Test-only escape hatch naming extra permitted endpoint hosts, comma-separated, so the integration
     * suites can point a data source at a local fixture instead of at AWS. A host named here bypasses this
     * check entirely, scheme included, because the fixtures serve plain http on a loopback address.
     *
     * <p>Read only in a snapshot build, which is also the only build the S3 integration suites run in
     * ({@code javaRestTest} is disabled otherwise), so in a release build the property has no effect at all
     * and there is no way to widen the constraint on a shipped node. Scoped to this plugin so it cannot
     * collide with {@code repository-s3}. Precedent for the shape:
     * {@code CustomWebIdentityTokenCredentialsProvider.STS_ENDPOINT_OVERRIDE_PROPERTY}.
     *
     * <p>Suites set it from the fixture's own address. The name is repeated as a literal in the two test
     * modules that need it — {@code SeedingS3HttpFixture} here and {@code S3FixtureUtils} in the esql qa
     * modules — because neither source set can see this class; {@code S3EndpointCheckTests} pins the value
     * so a rename cannot silently leave those behind.
     */
    static final String ADDITIONAL_HOSTS_PROPERTY = "org.elasticsearch.xpack.esql.datasource.s3.additionalEndpointHosts";

    /**
     * Every DNS suffix reachable from the SDK's partition metadata, longest-match-first at lookup time.
     * Enumerated over {@link Region#regions()} — a fixed built-in list that {@link Region#of} does not
     * extend — crossed with the four FIPS/dual-stack tag combinations, because a partition spells a
     * dual-stack destination under a different suffix ({@code api.aws}, {@code api.amazonwebservices.com.cn}).
     */
    private static final Set<String> PARTITION_DNS_SUFFIXES;

    /** The region-token patterns the SDK's partitions declare, used to require a region after the service label. */
    private static final List<Pattern> REGION_PATTERNS;

    static {
        List<PartitionEndpointKey> endpointKeys = List.of(
            PartitionEndpointKey.builder().tags(Set.of()).build(),
            PartitionEndpointKey.builder().tags(Set.of(EndpointTag.FIPS)).build(),
            PartitionEndpointKey.builder().tags(Set.of(EndpointTag.DUALSTACK)).build(),
            PartitionEndpointKey.builder().tags(Set.of(EndpointTag.FIPS, EndpointTag.DUALSTACK)).build()
        );
        Set<String> suffixes = new TreeSet<>();
        Set<String> regionRegexes = new LinkedHashSet<>();
        for (Region region : Region.regions()) {
            PartitionMetadata partition = PartitionMetadata.of(region);
            regionRegexes.add(partition.regionRegex());
            for (PartitionEndpointKey key : endpointKeys) {
                String suffix = partition.dnsSuffix(key);
                if (Strings.hasText(suffix)) {
                    suffixes.add(suffix.toLowerCase(Locale.ROOT));
                }
            }
        }
        PARTITION_DNS_SUFFIXES = Set.copyOf(suffixes);
        REGION_PATTERNS = regionRegexes.stream().map(Pattern::compile).toList();
    }

    private S3EndpointCheck() {}

    /**
     * Validates both endpoint settings on a data source, adding one error per refused value. Does not throw.
     *
     * <p>A blank or absent value is valid: it means the SDK resolves the endpoint itself from the region,
     * which is the confined path by construction.
     */
    static void validate(S3Configuration config, ValidationException errors) {
        validateEndpoint(config.endpoint(), "endpoint", S3_SERVICE, errors);
        validateEndpoint(config.stsEndpoint(), "sts_endpoint", STS_SERVICE, errors);
    }

    private static void validateEndpoint(String value, String settingName, String service, ValidationException errors) {
        if (Strings.hasText(value) == false) {
            return;
        }
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            // The shared URL check has already recorded a parse failure on this value.
            return;
        }
        if (uri.getHost() == null) {
            // Likewise: a value with no parseable host is already refused for being a malformed URL.
            return;
        }
        if (additionalHosts().contains(uri.getHost().toLowerCase(Locale.ROOT))) {
            // Test-only fixture route. It waives the scheme check as well as the host check, because the
            // integration fixtures serve plain http on a loopback address.
            return;
        }
        if ("https".equalsIgnoreCase(uri.getScheme()) == false) {
            // The host rule rests on the certificate presented for that name. Over plain http nothing
            // binds the name to its owner, so the rule would confine nothing.
            errors.addValidationError(settingName + " [" + value + "] must use https; plain http does not authenticate the endpoint");
            return;
        }
        if (isPermittedHost(uri.getHost(), service) == false) {
            errors.addValidationError(
                settingName
                    + " ["
                    + value
                    + "] names a host that is not a supported AWS "
                    + service.toUpperCase(Locale.ROOT)
                    + " endpoint. Use a regional endpoint (for example https://"
                    + service
                    + ".us-east-1.amazonaws.com), an AWS VPC interface endpoint, or omit the setting to have "
                    + "the endpoint resolved from the region"
            );
        }
    }

    /**
     * Whether {@code host} is an AWS endpoint of {@code service}. Package-private so the spelling-attack
     * cases can drive it directly.
     */
    static boolean isPermittedHost(String host, String service) {
        if (host == null || host.isEmpty()) {
            return false;
        }
        String normalized = host.toLowerCase(Locale.ROOT);
        // A fully-qualified name may carry a root dot; DNS treats it as the same name, so strip it
        // rather than refusing a value that resolves identically.
        if (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        // Longest match, so amazonaws.com.cn is not consumed as amazonaws.com with a stray "cn" label.
        String suffix = null;
        for (String candidate : PARTITION_DNS_SUFFIXES) {
            if (normalized.endsWith("." + candidate) && (suffix == null || candidate.length() > suffix.length())) {
                suffix = candidate;
            }
        }
        if (suffix == null) {
            return false;
        }
        String prefix = normalized.substring(0, normalized.length() - suffix.length() - 1);
        if (prefix.isEmpty()) {
            return false;
        }
        String[] labels = prefix.split("\\.", -1);
        for (String label : labels) {
            if (label.isEmpty()) {
                return false;
            }
        }
        return isServiceEndpoint(labels, service) || isVpcInterfaceEndpoint(labels, service);
    }

    /** {@code <service>[.dualstack].<region>} before the suffix, or the global {@code <service>}. */
    private static boolean isServiceEndpoint(String[] labels, String service) {
        if (isServiceLabel(labels[0], service) == false) {
            return false;
        }
        int next = 1;
        if (next < labels.length && labels[next].equals("dualstack")) {
            next++;
        }
        if (next == labels.length) {
            return true;
        }
        return next == labels.length - 1 && isRegionLabel(labels[next]);
    }

    /**
     * {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce} before the suffix. Requiring the service name
     * immediately after the endpoint id is what separates an AWS-operated interface endpoint for this
     * service from a customer-published PrivateLink service, whose second label is {@code vpce-svc-<id>}.
     */
    private static boolean isVpcInterfaceEndpoint(String[] labels, String service) {
        if (labels[labels.length - 1].equals("vpce") == false) {
            return false;
        }
        for (int i = 0; i + 1 < labels.length; i++) {
            if (labels[i].startsWith("vpce-") && labels[i + 1].equals(service)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The leading label of a service endpoint. It carries the service and whatever variant the resolver
     * spells alongside it: {@code s3-fips}, {@code s3-accesspoint}, {@code sts-fips}, and the directory-bucket
     * form {@code s3express-<az>}, which the resolver writes without the separating dash.
     */
    private static boolean isServiceLabel(String label, String service) {
        return label.equals(service) || label.startsWith(service + "-") || (S3_SERVICE.equals(service) && label.startsWith("s3express-"));
    }

    private static boolean isRegionLabel(String label) {
        for (Pattern pattern : REGION_PATTERNS) {
            if (pattern.matcher(label).matches()) {
                return true;
            }
        }
        return false;
    }

    @SuppressForbidden(reason = "test-only fixture route, mirroring the STS endpoint override system property")
    private static Set<String> additionalHosts() {
        if (Build.current().isSnapshot() == false) {
            return Set.of();
        }
        String configured = System.getProperty(ADDITIONAL_HOSTS_PROPERTY);
        if (Strings.hasText(configured) == false) {
            return Set.of();
        }
        return Arrays.stream(configured.split(","))
            .map(host -> host.trim().toLowerCase(Locale.ROOT))
            .filter(host -> host.isEmpty() == false)
            .collect(Collectors.toUnmodifiableSet());
    }
}
