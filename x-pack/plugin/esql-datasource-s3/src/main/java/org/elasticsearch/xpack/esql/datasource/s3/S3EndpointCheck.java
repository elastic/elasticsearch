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
 * <p>Both settings become an endpoint override on an AWS SDK client builder, so whatever host they name is
 * one this node connects to. For {@code sts_endpoint} that host receives the node's own OIDC token as a
 * bearer credential, because the STS client authenticates with nothing else.
 *
 * <p>A regional endpoint is matched by <em>membership</em> rather than by parsing: {@link #S3_ENDPOINT_HOSTS}
 * and {@link #STS_ENDPOINT_HOSTS} are built at class load by crossing the enabled service labels with every
 * region the SDK knows, so admitting a host means finding it in a set the SDK's own metadata generated. A
 * region the pinned SDK has never heard of is therefore refused, not pattern-matched — {@code us-east-99} is
 * a well-formed region token and names nothing. That is the intended direction: a new AWS region is readmitted
 * by an SDK upgrade, and in the meantime by an operator naming the host in
 * {@code esql.external.allowed_endpoint_hosts}.
 *
 * <p>PrivateLink is the one shape with no such source — {@code vpce} appears in none of the SDK jars, and the
 * endpoint id is minted per customer — so {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce} is matched
 * against a generated tail, leaving only the id and its optional prefix to be read from the name.
 *
 * <p>The two global endpoints, {@code s3.amazonaws.com} and {@code sts.amazonaws.com}, are admitted as
 * literals. They name no region, so they reach a {@code us-east-1} bucket and fail loudly for any other,
 * because the cross-region redirect S3 answers with is not followed.
 *
 * <p>Requiring a region after the service label is load-bearing, and the global literals do not weaken it:
 * without that requirement a bucket anyone can create named {@code sts-anything} answers to
 * {@code sts-anything.s3.us-east-1.amazonaws.com} and would be admitted. Every other AWS endpoint family is
 * refused — see {@link #S3_SERVICE_LABELS}.
 */
final class S3EndpointCheck {

    static final String S3_SERVICE = "s3";
    static final String STS_SERVICE = "sts";

    /**
     * Every S3 endpoint family AWS serves, with only the regional object endpoint enabled. The list is
     * load-bearing rather than documentary: each enabled label is crossed with every region to build
     * {@link #S3_ENDPOINT_HOSTS}. Uncommenting a line is the first step in readmitting a family, not the
     * whole of it — {@code s3express} carries an availability-zone token rather than a fixed label, so its
     * entry is a placeholder and not something that compiles as written.
     *
     * <p>The historical {@code s3-<region>} spelling is absent because it is not a label of its own — it
     * carries the region inside the service label, and is generated separately. Dual-stack is absent because
     * it is a label between the service and the region, so no host built from this list can contain it.
     *
     * <p>{@code s3-fips} and dual-stack are disabled for the same reason as the rest: nothing here has been
     * tested against them. Naming one does reach it — AWS serves both, in commercial regions as well as in
     * GovCloud — so this is a decision about what is supported rather than a technical limit.
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
     * The fixed tails of a PrivateLink interface endpoint, {@code .<service>.<region>.vpce.<suffix>}, one per
     * region. What precedes a tail is the customer's endpoint id and its optional prefix, which is all this
     * class reads out of such a name.
     */
    private static final Set<String> S3_VPCE_TAILS;
    private static final Set<String> STS_VPCE_TAILS;

    static {
        Set<String> s3Hosts = new TreeSet<>();
        Set<String> stsHosts = new TreeSet<>();
        Set<String> s3Tails = new TreeSet<>();
        Set<String> stsTails = new TreeSet<>();
        for (Region region : Region.regions()) {
            // The pseudo-regions Region.regions() also carries are an input to the SDK's resolver rather
            // than endpoints anyone configures, and each resolves to a region-less global host, which this
            // class refuses for naming no region.
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
            // The historical spelling, still resolvable and still in customer configuration.
            s3Hosts.add(S3_SERVICE + "-" + id + "." + suffix);
            s3Tails.add("." + S3_SERVICE + "." + id + ".vpce." + suffix);
            stsTails.add("." + STS_SERVICE + "." + id + ".vpce." + suffix);
        }
        // The global endpoints, which name no region. S3 answers for us-east-1 and redirects anywhere else
        // with a cross-region 301 this data source does not follow, so they reach a us-east-1 bucket and fail
        // loudly for any other. Only the bare service label has a global form; an enabled family does not
        // acquire one. They are literals rather than a relaxation: admitting them adds exactly these two
        // names, and the requirement that a region follow the service label — which is what refuses a bucket
        // named sts-anything answering on sts-anything.s3.us-east-1.amazonaws.com — is untouched.
        String globalSuffix = PartitionMetadata.of(Region.AWS_GLOBAL).dnsSuffix();
        if (Strings.hasText(globalSuffix)) {
            globalSuffix = globalSuffix.toLowerCase(Locale.ROOT);
            s3Hosts.add(S3_SERVICE + "." + globalSuffix);
            stsHosts.add(STS_SERVICE + "." + globalSuffix);
        }
        if (s3Hosts.isEmpty() || stsHosts.isEmpty()) {
            // Both come from SDK metadata. Empty would silently refuse every endpoint value on the node,
            // which reads as a product outage rather than as a missing dependency. This class is first
            // touched by a data-source PUT, so the failure surfaces there rather than at node startup.
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
        return endpointHosts(service).contains(normalized) || isVpcInterfaceEndpoint(normalized, service);
    }

    /**
     * {@code [<prefix>.]vpce-<id>.<service>.<region>.vpce} under the region's own partition suffix. The
     * service and region come from the matched tail, so the only thing read out of the name is the endpoint
     * id and its optional prefix, each of which must be a single label.
     *
     * <p>What excludes a customer-published PrivateLink service is the tail, not the {@code vpce-svc-} test:
     * AWS mints such an endpoint as {@code vpce-<id>-<hash>.vpce-svc-<serviceid>.<region>.vpce.<suffix>},
     * whose label before the region is the service id rather than {@code s3} or {@code sts}, so no generated
     * tail matches it. The {@code vpce-svc-} test is defence in depth against a spelling AWS does not mint;
     * it is kept because the tail is the only thing holding that line and one guard is not enough for it.
     *
     * <p>The tail names the bare service, so an interface endpoint for one of the other S3 families —
     * {@code s3-outposts}, say — is refused even if that family is later enabled above. No source of truth
     * here establishes what AWS serves for those, and refusing a form nobody has confirmed is the direction
     * that cannot admit a host we did not mean to reach.
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
