/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.regions.PartitionMetadata;
import software.amazon.awssdk.regions.Region;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;

import java.net.URI;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Restricts the {@code endpoint} and {@code sts_endpoint} data-source settings at
 * {@code PUT /_query/data_source} time, via
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withDatasourceCheck}.
 *
 * <p>A host is admitted by membership in sets built at class load from the SDK's region metadata, or by matching
 * the PrivateLink shape, so a region the pinned SDK does not know is refused. A region must follow the service label: without
 * that, a bucket named {@code sts-anything} would answer to {@code sts-anything.s3.us-east-1.amazonaws.com}.
 */
final class S3EndpointCheck {

    static final String S3_SERVICE = "s3";
    static final String STS_SERVICE = "sts";

    // Only the regional object endpoint is enabled here; the historical and global spellings are added below.
    // tag::
    private static final Set<String> S3_SERVICE_LABELS = Set.of(
        "s3"                        // the regional object endpoint
     // "s3-fips",                  // the same over FIPS 140-validated cryptography
     // "s3-accesspoint",           // an access point, fronting one bucket under its own policy
     // "s3-accesspoint-fips",
     // "s3-accelerate",            // transfer acceleration, routed via an edge location
     // "s3-object-lambda",         // an access point running a Lambda over each object as it is read
     // "s3-object-lambda-fips",
     // "s3-outposts",              // storage on an Outposts rack in the customer's own data centre
     // "s3-outposts-fips",
     // "s3-control",               // the account-level control plane, which serves no object reads
     // "s3-control-fips",
     // "s3-external-1",            // the legacy us-east-1 alias
     // "s3express-<az>",           // S3 Express, whose buckets S3ResourceCheck refuses by name too
     // "s3express-fips-<az>",      // the same over FIPS 140-validated cryptography
    );
    // end::

    // tag::
    private static final Set<String> STS_SERVICE_LABELS = Set.of(
        "sts"                       // the regional token service
     // "sts-fips",                 // the same over FIPS 140-validated cryptography
    );
    // end::

    private static final Set<String> S3_ENDPOINT_HOSTS;
    private static final Set<String> STS_ENDPOINT_HOSTS;
    /** PrivateLink tails, {@code .<service>.<region>.vpce.<suffix>}; the customer's endpoint id precedes one. */
    private static final Set<String> S3_VPCE_TAILS;
    private static final Set<String> STS_VPCE_TAILS;

    static {
        Set<String> s3Hosts = new HashSet<>();
        Set<String> stsHosts = new HashSet<>();
        Set<String> s3Tails = new HashSet<>();
        Set<String> stsTails = new HashSet<>();
        for (Region region : Region.regions()) {
            if (region.isGlobalRegion()) {
                continue;
            }
            String id = region.id().toLowerCase(Locale.ROOT);
            String suffix = PartitionMetadata.of(region).dnsSuffix();
            if (Strings.hasText(suffix) == false) {
                continue;
            }
            suffix = suffix.toLowerCase(Locale.ROOT);
            for (String label : S3_SERVICE_LABELS) {
                s3Hosts.add(label + "." + id + "." + suffix);
            }
            for (String label : STS_SERVICE_LABELS) {
                stsHosts.add(label + "." + id + "." + suffix);
            }
            // The historical spelling, generated for every region; some names resolve to nothing, but each is
            // built from SDK metadata rather than from the configured value.
            s3Hosts.add(S3_SERVICE + "-" + id + "." + suffix);
            s3Tails.add("." + S3_SERVICE + "." + id + ".vpce." + suffix);
            stsTails.add("." + STS_SERVICE + "." + id + ".vpce." + suffix);
        }
        // The global endpoints name no region but pin the host: with an endpoint set, cross-region redirects are
        // not followed, so one reaches us-east-1 buckets and fails for others rather than being sent on.
        String globalSuffix = PartitionMetadata.of(Region.AWS_GLOBAL).dnsSuffix();
        if (Strings.hasText(globalSuffix)) {
            s3Hosts.add(S3_SERVICE + "." + globalSuffix.toLowerCase(Locale.ROOT));
            stsHosts.add(STS_SERVICE + "." + globalSuffix.toLowerCase(Locale.ROOT));
        }
        S3_ENDPOINT_HOSTS = Set.copyOf(s3Hosts);
        STS_ENDPOINT_HOSTS = Set.copyOf(stsHosts);
        S3_VPCE_TAILS = Set.copyOf(s3Tails);
        STS_VPCE_TAILS = Set.copyOf(stsTails);
    }

    private S3EndpointCheck() {}

    /** Adds one error per refused value. A blank value is valid: the SDK then resolves from the region. */
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
        // S3Configuration.validateSettings refuses these first; refuse rather than permit what this cannot read.
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
        boolean https = "https".equalsIgnoreCase(uri.getScheme());
        // The STS host receives the node's OIDC token, so it gets TLS even when the operator allowlist names it.
        if (https == false && STS_SERVICE.equals(service)) {
            errors.addValidationError(settingName + " [" + value + "] must use https; it is sent the node's OIDC token");
            return;
        }
        if (allowedByOperator.test(hostAndPort(uri))) {
            return;
        }
        if (https == false) {
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

    /** The allowlist matches a lower-cased {@code host:port}; the AWS host rule below ignores the port. */
    private static String hostAndPort(URI uri) {
        int port = uri.getPort();
        if (port == -1) {
            port = "http".equalsIgnoreCase(uri.getScheme()) ? 80 : 443;
        }
        return uri.getHost().toLowerCase(Locale.ROOT) + ":" + port;
    }

    /** Package-private: {@link S3ResourceCheck} uses it too. */
    static boolean isPermittedHost(String host, String service) {
        if (host == null || host.isEmpty()) {
            return false;
        }
        String normalized = host.toLowerCase(Locale.ROOT);
        if (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return endpointHosts(service).contains(normalized) || isVpcInterfaceEndpoint(normalized, service);
    }

    /**
     * {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce.<suffix>}. Only the id and, for S3, a single-label prefix
     * are read from the name; STS endpoints carry no prefix. {@code vpce-svc-} is refused because it names a
     * customer-published service, which anyone can mint.
     */
    private static boolean isVpcInterfaceEndpoint(String host, String service) {
        for (String tail : vpceTails(service)) {
            if (host.endsWith(tail) == false) {
                continue;
            }
            String head = host.substring(0, host.length() - tail.length());
            int lastDot = head.lastIndexOf('.');
            String id = head.substring(lastDot + 1);
            String prefix = lastDot < 0 ? "" : head.substring(0, lastDot);
            boolean prefixAllowed = lastDot < 0 || (S3_SERVICE.equals(service) && isSingleLabel(prefix));
            if (id.length() > "vpce-".length() && id.startsWith("vpce-") && id.startsWith("vpce-svc-") == false && prefixAllowed) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSingleLabel(String value) {
        return value.isEmpty() == false && value.indexOf('.') < 0;
    }

    private static Set<String> endpointHosts(String service) {
        return switch (service) {
            case S3_SERVICE -> S3_ENDPOINT_HOSTS;
            case STS_SERVICE -> STS_ENDPOINT_HOSTS;
            default -> throw new IllegalArgumentException("unknown service [" + service + "]");
        };
    }

    private static Set<String> vpceTails(String service) {
        return switch (service) {
            case S3_SERVICE -> S3_VPCE_TAILS;
            case STS_SERVICE -> STS_VPCE_TAILS;
            default -> throw new IllegalArgumentException("unknown service [" + service + "]");
        };
    }

}
