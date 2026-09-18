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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Restricts the {@code endpoint} and {@code sts_endpoint} data-source settings to the AWS endpoints this
 * product supports, at {@code PUT /_query/data_source} time via
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withDatasourceCheck}.
 *
 * <p>Both settings become an endpoint override on an AWS SDK client builder, so whatever host they name is
 * one this node connects to. For {@code sts_endpoint} that host receives the node's own OIDC token as a
 * bearer credential, because the STS client authenticates with nothing else.
 *
 * <p>A host is permitted in one of two shapes, under an AWS partition DNS suffix, and both name a region:
 *
 * <ul>
 *   <li>{@code <service>[.dualstack].<region>}, or the historical {@code s3-<region>} spelling that carries
 *       its region in the service label — see {@link #S3_SERVICE_LABELS} for which leading labels qualify
 *       and which endpoint families are deliberately excluded.</li>
 *   <li>{@code [<prefix>.]vpce-<id>.<service>.<region>.vpce} — an AWS PrivateLink interface endpoint, the
 *       one destination a customer cannot express any other way.</li>
 * </ul>
 *
 * <p>Regional and PrivateLink are the whole permitted set. Every other AWS endpoint family — the global
 * endpoint, transfer acceleration, access points, object lambda, Outposts, the control plane, the legacy
 * alias and S3 Express — is refused, and reaching one takes an operator naming its host in
 * {@code esql.external.allowed_endpoint_hosts}. A knob per family would be a second lever over the same
 * rule, overridable by that list and therefore not a constraint at all.
 *
 * <p>Suffixes and region patterns come from the SDK's partition metadata, so a partition it gains needs no
 * change here. The interface-endpoint shape has no such source — {@code vpce} appears in none of the SDK
 * jars — so it is maintained here and pinned by {@code S3EndpointCheckTests}.
 *
 * <p>Requiring a region after the service label is load-bearing. Without it any name whose leading label
 * merely starts with the service prefix is admitted, and a bucket called {@code sts-anything} answers to
 * {@code sts-anything.s3.us-east-1.amazonaws.com}.
 */
final class S3EndpointCheck {

    static final String S3_SERVICE = "s3";
    static final String STS_SERVICE = "sts";

    /**
     * The leading label of a regional S3 object endpoint, plain or FIPS. Beside these there is one form
     * whose tail is generated rather than fixed, handled by pattern in {@link #isServiceLabel}: the
     * historical {@code s3-<region>} spelling, which carries its region in the service label itself.
     *
     * <p>Every other S3 endpoint family AWS serves is deliberately absent, and each is reachable only by an
     * operator naming its host in {@code esql.external.allowed_endpoint_hosts}:
     *
     * <ul>
     *   <li>{@code s3-accesspoint}, {@code s3-accesspoint-fips} — an access point, which fronts one bucket
     *       under its own policy.</li>
     *   <li>{@code s3-accelerate} — transfer acceleration, which routes through an edge location.</li>
     *   <li>{@code s3-object-lambda}, {@code s3-object-lambda-fips} — an access point that runs a Lambda over
     *       each object as it is read.</li>
     *   <li>{@code s3-outposts}, {@code s3-outposts-fips} — storage on an Outposts rack in the customer's own
     *       data centre.</li>
     *   <li>{@code s3-control}, {@code s3-control-fips} — the account-level control plane (access points,
     *       jobs), which serves no object reads at all.</li>
     *   <li>{@code s3-external-1} — the legacy {@code us-east-1} alias, still resolvable.</li>
     *   <li>{@code s3express-<az>} — S3 Express, which serves only directory buckets; those are refused by
     *       name in {@link S3ResourceCheck} as well, because the bucket name alone moves the destination.</li>
     * </ul>
     */
    private static final Set<String> S3_SERVICE_LABELS = Set.of("s3", "s3-fips");

    /** The leading label of a regional STS endpoint: the token service, plain or FIPS. */
    private static final Set<String> STS_SERVICE_LABELS = Set.of("sts", "sts-fips");

    /**
     * Enumerated over {@link Region#regions()} — a fixed built-in list {@link Region#of} does not extend —
     * crossed with the four FIPS/dual-stack combinations, because a partition spells its dual-stack
     * destination under a different suffix ({@code api.aws}, {@code api.amazonwebservices.com.cn}).
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
        if (suffixes.isEmpty() || regionRegexes.isEmpty()) {
            // Both come from SDK metadata. Empty would silently refuse every endpoint value on the node,
            // which reads as a product outage rather than as a missing dependency; fail at load instead.
            throw new IllegalStateException(
                "no AWS partition metadata available: suffixes=" + suffixes + " regionPatterns=" + regionRegexes
            );
        }
        PARTITION_DNS_SUFFIXES = Set.copyOf(suffixes);
        REGION_PATTERNS = regionRegexes.stream().map(Pattern::compile).toList();
    }

    private S3EndpointCheck() {}

    /**
     * Adds one error per refused value; does not throw. A blank or absent value is valid — the SDK then
     * resolves the endpoint from the region, which is confined by construction.
     */
    static void validate(S3Configuration config, Predicate<String> allowedByOperator, ValidationException errors) {
        validateEndpoint(config.endpoint(), "endpoint", S3_SERVICE, allowedByOperator, errors);
        validateEndpoint(config.stsEndpoint(), "sts_endpoint", STS_SERVICE, allowedByOperator, errors);
    }

    private static void validateEndpoint(
        String value,
        String settingName,
        String service,
        Predicate<String> allowedByOperator,
        ValidationException errors
    ) {
        if (Strings.hasText(value) == false) {
            return;
        }
        // Both refusals below are unreachable today: the shared URL check in S3Configuration.validateSettings
        // throws on an unparseable value, and on one with no host, before the configuration object this runs
        // on exists. The invariant lives in another class, so this refuses rather than permits — a value this
        // method cannot read is one it cannot vouch for.
        URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException e) {
            errors.addValidationError(settingName + " [" + value + "] is not a valid URL");
            return;
        }
        if (uri.getHost() == null) {
            errors.addValidationError(settingName + " [" + value + "] names no host");
            return;
        }
        if (allowedByOperator.test(hostAndPort(uri))) {
            // Named by the node's own configuration, so the scheme is not examined either — see
            // ExternalSourceSettings#ALLOWED_ENDPOINT_HOSTS. The match is exact where the AWS rule below
            // normalises: an operator writes the host as the SDK will send it, so a differing case or a
            // trailing root dot falls through to that rule rather than being waived here.
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
     * The {@code host:port} the allowlist is matched against, with the scheme's default port supplied when the
     * value carries none, so an entry never has to guess which spelling the user wrote.
     */
    private static String hostAndPort(URI uri) {
        int port = uri.getPort();
        if (port == -1) {
            port = "http".equalsIgnoreCase(uri.getScheme()) ? 80 : 443;
        }
        return uri.getHost() + ":" + port;
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
        boolean dualStack = next < labels.length && labels[next].equals("dualstack");
        if (dualStack) {
            next++;
        }
        if (next == labels.length) {
            // Nothing after the service label, so no region was named. The historical s3-<region> spelling
            // carries its region inside that label and is the one form that reaches here legitimately; the
            // region-less global endpoints (s3.amazonaws.com, sts.amazonaws.com) are refused with the rest
            // of the non-regional families.
            return isDashRegionLabel(labels[0]);
        }
        return next == labels.length - 1 && isRegionLabel(labels[next]);
    }

    /**
     * {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce}, every part in a fixed position. The service
     * immediately after the endpoint id is what separates an AWS-operated interface endpoint from a
     * customer-published PrivateLink service, whose label there is {@code vpce-svc-<id>} — a spelling that
     * satisfies the {@code vpce-} test too, so position rejects it rather than the prefix.
     *
     * <p>The comparison is against the bare service name, so an interface endpoint for one of the other S3
     * service labels — {@code s3-outposts}, say — is refused even though that label is accepted in a service
     * endpoint. That is deliberate rather than an oversight: no source of truth here establishes what AWS
     * serves for those, and refusing a form nobody has confirmed is the direction that cannot admit a host
     * we did not mean to reach.
     */
    private static boolean isVpcInterfaceEndpoint(String[] labels, String service) {
        // <id>.<service>.<region>.vpce, optionally preceded by one label such as bucket/accesspoint/control.
        int start = labels.length - 4;
        if (start != 0 && start != 1) {
            return false;
        }
        return labels[start].startsWith("vpce-")
            && labels[start].startsWith("vpce-svc-") == false
            && labels[start + 1].equals(service)
            && isRegionLabel(labels[start + 2])
            && labels[start + 3].equals("vpce");
    }

    /**
     * The leading label of a service endpoint.
     *
     * <p>Enumerated rather than prefix-matched: a prefix test admits the open set of names beginning
     * {@code s3-} or {@code sts-}, which is wider than anything AWS serves.
     */
    private static boolean isServiceLabel(String label, String service) {
        return switch (service) {
            case S3_SERVICE -> S3_SERVICE_LABELS.contains(label) || isDashRegionLabel(label);
            case STS_SERVICE -> STS_SERVICE_LABELS.contains(label);
            default -> throw new IllegalArgumentException("unknown service [" + service + "]");
        };
    }

    /** The historical {@code s3-<region>} spelling, still resolvable and still in customer configuration. */
    private static boolean isDashRegionLabel(String label) {
        return label.startsWith("s3-") && isRegionLabel(label.substring(3));
    }

    private static boolean isRegionLabel(String label) {
        for (Pattern pattern : REGION_PATTERNS) {
            if (pattern.matcher(label).matches()) {
                return true;
            }
        }
        return false;
    }

}
