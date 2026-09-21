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
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * Restricts the {@code endpoint} and {@code sts_endpoint} data-source settings at
 * {@code PUT /_query/data_source} time, via
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator#withDatasourceCheck}.
 *
 * <p>Both settings become an endpoint override on an SDK client builder, so whatever host they name is one
 * this node connects to — and for {@code sts_endpoint} that host receives the node's own OIDC token, the
 * STS client having nothing else to authenticate with.
 *
 * <p>A host is admitted by <em>membership</em>, not by parsing: {@link #S3_ENDPOINT_HOSTS} and
 * {@link #STS_ENDPOINT_HOSTS} are built at class load by crossing the enabled service labels with every
 * region the SDK knows, so a region the pinned SDK has not heard of is refused rather than pattern-matched,
 * and readmitted by an SDK upgrade or meanwhile by an operator naming the host in
 * {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings#ALLOWED_ENDPOINT_HOSTS}.
 * PrivateLink is the one shape with no such source, so it is matched against a generated tail with only the
 * endpoint id and its prefix read from the name. The two global endpoints are literals.
 *
 * <p>Everywhere else a region must follow the service label, which is load-bearing: without it a bucket
 * anyone can create named {@code sts-anything} answers to {@code sts-anything.s3.us-east-1.amazonaws.com}.
 * Every other family is refused — see {@link #S3_SERVICE_LABELS}.
 */
final class S3EndpointCheck {

    static final String S3_SERVICE = "s3";
    static final String STS_SERVICE = "sts";

    /**
     * Every S3 endpoint family AWS serves, with only the regional object endpoint enabled: each enabled
     * label is crossed with every region to build {@link #S3_ENDPOINT_HOSTS}. Uncommenting a line is the
     * first step in readmitting a family, never the whole of it — the enabled element carries no trailing
     * comma, {@code s3express} carries an availability-zone token rather than a fixed label to enable, and
     * {@code s3-external-1} and the acceleration forms are not spelled {@code <label>.<region>.<suffix>},
     * which is the only shape this crossing builds. The historical {@code s3-<region>} spelling is absent
     * because it carries the region inside the label and is generated separately below; dual-stack because
     * it sits between service and region, so no host built from this list can carry it.
     *
     * <p>All are disabled because nothing has been tested against them, not because they cannot be reached.
     */
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

    /** The STS endpoint families, only the regional one enabled. {@code sts-fips} is disabled for S3's reason. */
    // tag::
    private static final Set<String> STS_SERVICE_LABELS = Set.of(
        "sts"                       // the regional token service
     // "sts-fips",                 // the same over FIPS 140-validated cryptography
    );
    // end::

    /** Every permitted S3 host, in full. Built below; matched by equality. */
    private static final Set<String> S3_ENDPOINT_HOSTS;

    /** Every permitted STS host, in full. */
    private static final Set<String> STS_ENDPOINT_HOSTS;

    /**
     * The fixed tails of a PrivateLink interface endpoint, {@code .<service>.<region>.vpce.<suffix>}, one
     * per region. What precedes a tail is the customer's endpoint id and its optional prefix.
     */
    private static final Set<String> S3_VPCE_TAILS;
    private static final Set<String> STS_VPCE_TAILS;

    static {
        Set<String> s3Hosts = new TreeSet<>();
        Set<String> stsHosts = new TreeSet<>();
        Set<String> s3Tails = new TreeSet<>();
        Set<String> stsTails = new TreeSet<>();
        for (Region region : Region.regions()) {
            // The pseudo-regions Region.regions() also carries are resolver input, not endpoints anyone
            // configures, and each is region-less, which this class refuses anyway.
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
            // The historical spelling. Generated for every region rather than the ones that serve it, so
            // some entries resolve to nothing — but each is still built from SDK metadata rather than from
            // anything the configured value carried, which is the property the rule rests on.
            s3Hosts.add(S3_SERVICE + "-" + id + "." + suffix);
            s3Tails.add("." + S3_SERVICE + "." + id + ".vpce." + suffix);
            stsTails.add("." + STS_SERVICE + "." + id + ".vpce." + suffix);
        }
        // The global endpoints. Only the bare service label has one. Naming no region does not relax the
        // region requirement: with an endpoint set, cross-region access is off and the discovered-region
        // retry re-signs against the same host, so one reaches us-east-1 and fails elsewhere rather than
        // being sent on. This lookup answers for the commercial
        // partition whichever global pseudo-region it is given, so it is not partition-aware — which matters
        // the moment another partition's global form is admitted.
        String globalSuffix = PartitionMetadata.of(Region.AWS_GLOBAL).dnsSuffix();
        if (Strings.hasText(globalSuffix) == false) {
            // Unreachable at the pinned SDK; dropping the global literals in silence would refuse a value
            // the docs list as accepted.
            throw new IllegalStateException("no global AWS partition suffix available");
        }
        globalSuffix = globalSuffix.toLowerCase(Locale.ROOT);
        s3Hosts.add(S3_SERVICE + "." + globalSuffix);
        stsHosts.add(STS_SERVICE + "." + globalSuffix);
        if (s3Hosts.isEmpty() || stsHosts.isEmpty()) {
            // Both come from SDK metadata. Empty would silently refuse every endpoint on the node, which
            // reads as an outage rather than a missing dependency. This class is first touched by a
            // data-source PUT, so the failure surfaces there rather than at node startup.
            throw new IllegalStateException("no AWS partition metadata available");
        }
        S3_ENDPOINT_HOSTS = Set.copyOf(s3Hosts);
        STS_ENDPOINT_HOSTS = Set.copyOf(stsHosts);
        S3_VPCE_TAILS = Set.copyOf(s3Tails);
        STS_VPCE_TAILS = Set.copyOf(stsTails);
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
        // Unreachable today: S3Configuration.validateSettings throws on an unparseable value, and on one
        // with no host, before this runs. That invariant lives in another class, so refuse rather than
        // permit what this cannot read.
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
            // Named by the node's own configuration, so the scheme is not examined either. The match is
            // not normalised, unlike the AWS rule below, so a differing case or a trailing root dot falls
            // through to that rule rather than being waived here.
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
     * The {@code host:port} the allowlist is matched against, with the scheme's default port supplied when
     * the value carries none. Only the allowlist uses this; the AWS host rule matches on the host alone, so
     * {@code https://s3.us-east-1.amazonaws.com:8443} is accepted.
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
        // A fully-qualified name may carry a root dot; DNS treats it as the same name.
        if (normalized.endsWith(".")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return endpointHosts(service).contains(normalized) || isVpcInterfaceEndpoint(normalized, service);
    }

    /**
     * {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce} under the region's own partition suffix. The
     * service and region come from the matched tail, so the only thing read out of the name is the endpoint
     * id and its optional prefix, each of which must be a single label.
     *
     * <p>None of the four checks below may be dropped; {@code testRefusesVpcFormsOutsideTheExactShape}
     * holds a host that only that check refuses:
     *
     * <ul>
     *   <li>the tail — {@code vpce-0a1b.ec2.us-east-1.vpce.amazonaws.com}, naming another service</li>
     *   <li>requiring {@code vpce-} — {@code evil.s3.us-east-1.vpce.amazonaws.com}</li>
     *   <li>rejecting {@code vpce-svc-} — {@code vpce-svc-0c2d.s3.us-east-1.vpce.amazonaws.com}, the prefix
     *       of a customer-published service name, which is the one an attacker can mint</li>
     *   <li>the single-label limit — {@code a.b.vpce-0a1b.s3.us-east-1.vpce.amazonaws.com}</li>
     * </ul>
     *
     * <p>The tail names the bare service, so an interface endpoint for another S3 family is refused even if
     * that family is later enabled above: nothing here establishes what AWS serves for those.
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
            if (id.startsWith("vpce-") && id.startsWith("vpce-svc-") == false && (lastDot < 0 || isSingleLabel(prefix))) {
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
