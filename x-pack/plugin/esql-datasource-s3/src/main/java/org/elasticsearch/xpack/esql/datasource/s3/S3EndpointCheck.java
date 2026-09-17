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
import org.elasticsearch.common.network.InetAddresses;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
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
 * <p>A host is permitted in one of two shapes, under an AWS partition DNS suffix:
 *
 * <ul>
 *   <li>{@code <service>[.dualstack].<region>}, or the global {@code <service>} — see
 *       {@link #S3_SERVICE_LABELS} for the leading labels and what each reaches.</li>
 *   <li>{@code [<prefix>.]vpce-<id>.<service>.<region>.vpce} — an AWS PrivateLink interface endpoint, the
 *       one destination a customer cannot express any other way.</li>
 * </ul>
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
     * The leading label of every S3 endpoint AWS serves, and what each one reaches. Beside these there is
     * one form whose tail is generated rather than fixed, handled by pattern in {@link #isServiceLabel}:
     * the historical {@code s3-<region>} spelling. The {@code s3express-<az>} label is deliberately absent —
     * it serves only directory buckets, which {@link S3ResourceCheck} refuses.
     */
    private static final Set<String> S3_SERVICE_LABELS = Set.of(
        "s3",                     // the regional and global object endpoint
        "s3-fips",                // the same, over a FIPS 140-validated endpoint
        "s3-accesspoint",         // an access point, which fronts one bucket under its own policy
        "s3-accesspoint-fips",
        "s3-accelerate",          // transfer acceleration, which routes through an edge location
        "s3-object-lambda",       // an access point that runs a Lambda over each object as it is read
        "s3-object-lambda-fips",
        "s3-outposts",            // storage on an Outposts rack in the customer's own data centre
        "s3-outposts-fips",
        "s3-control",             // the account-level control plane (access points, jobs), not object reads
        "s3-control-fips",
        "s3-external-1"           // the legacy us-east-1 alias, still resolvable
    );

    /** The leading label of every STS endpoint: the token service, regional or global, plain or FIPS. */
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
        if (isTestFixtureHost(uri.getHost())) {
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
        boolean dualStack = next < labels.length && labels[next].equals("dualstack");
        if (dualStack) {
            next++;
        }
        if (next == labels.length) {
            // The global form, the service label alone. Dual-stack is always regional, so the region
            // after it is required rather than optional.
            return dualStack == false;
        }
        return next == labels.length - 1 && isRegionLabel(labels[next]);
    }

    /**
     * {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce}, every part in a fixed position. The service
     * immediately after the endpoint id is what separates an AWS-operated interface endpoint from a
     * customer-published PrivateLink service, whose label there is {@code vpce-svc-<id>} — a spelling that
     * satisfies the {@code vpce-} test too, so position rejects it rather than the prefix.
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

    /**
     * Whether this is an integration fixture rather than a real destination. Every object-store fixture in
     * the repository binds a loopback address, and a snapshot build is the only build their suites run in,
     * so this is what lets them keep reading without each cluster naming its fixture somewhere. It is dead
     * code on a released node, which cannot reach a loopback service belonging to anyone but itself anyway.
     *
     * <p>An IP literal is recognised as loopback from the literal alone. The name {@code localhost} is
     * accepted as well, because {@code S3HttpFixture} hands one out in place of a literal on its TLS branch
     * on Windows; whether that name reaches a loopback destination is decided by the resolver, not here.
     * Both arms are reachable only on a snapshot build.
     */
    private static boolean isTestFixtureHost(String host) {
        if (Build.current().isSnapshot() == false) {
            return false;
        }
        String literal = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        return "localhost".equalsIgnoreCase(host)
            || (InetAddresses.isInetAddress(literal) && InetAddresses.forString(literal).isLoopbackAddress());
    }

}
