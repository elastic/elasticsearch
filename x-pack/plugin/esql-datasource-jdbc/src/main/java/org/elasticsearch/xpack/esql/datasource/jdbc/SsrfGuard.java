/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.network.InetAddresses;

import java.net.InetAddress;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Server-side request forgery guard for JDBC URLs. ES|QL exposes a primitive where any user with query privileges can
 * direct the coordinator to open a TCP connection to an arbitrary endpoint via {@code FROM "jdbc:..."}; without a
 * guard this is a free outbound-network probe (cloud metadata services, internal control planes, etc.). The guard
 * runs synchronously at {@link JdbcConnectorFactory#canHandle(String)} time so no socket is ever opened for a URL
 * we will not honour.
 * <p>
 * Two filters are applied in order:
 * <ol>
 *   <li><b>Subprotocol allowlist.</b> A URL must begin with one of the configured subprotocols
 *       ({@code jdbc:postgresql://}, {@code jdbc:mysql://}, ...). The default allowlist intentionally includes
 *       {@code jdbc:h2:mem:} so the in-process H2 we test against is permitted, and excludes file-backed schemes
 *       ({@code jdbc:h2:file:}, {@code jdbc:derby:directory:}, {@code jdbc:hsqldb:file:}, ...) which are not TCP
 *       at all and would let a query read arbitrary files relative to the ES process CWD.</li>
 *   <li><b>Loopback / link-local host rejection.</b> Even an allowed subprotocol must not point at the local host
 *       (lo, 127.0.0.0/8, ::1), link-local (169.254.0.0/16, fe80::/10), or wildcard / unspecified addresses.
 *       This blocks the most common SSRF target -- the cloud instance metadata endpoint at 169.254.169.254 --
 *       and protects against tunnelling to anything listening on the coordinator itself.</li>
 * </ol>
 * <p>
 * <b>What this guard does NOT do.</b>
 * <ul>
 *   <li>Resolve hostnames to validate the IP they map to. DNS resolution is an outbound network call that itself
 *       can be probed; doing it here would only push the SSRF primitive one hop. Operators who need IP-level
 *       enforcement should put a network policy in front of the coordinator.</li>
 *   <li>Block RFC1918 / private ranges. Many corporate JDBC deployments live behind private addressing; refusing
 *       them by default would break the common case. Operators who need to block private ranges should layer a
 *       network policy on top.</li>
 *   <li>Validate that the URL is well-formed. Malformed URLs that survive the prefix check are passed through to
 *       the driver, which will reject them. The guard's only job is to keep us from making the outbound call.</li>
 * </ul>
 * <p>
 * Both the subprotocol allowlist and the loopback-allowed flag are controlled by cluster settings so that test
 * deployments and dev environments can opt in to {@code jdbc:h2:mem:} (default) plus loopback (default off, but
 * forced on for unit-test in-process H2 because the alternative is no testable path through the connector).
 */
final class SsrfGuard {

    /**
     * Default subprotocol allowlist. {@code jdbc:h2:mem:} is included so the in-process H2 we ship tests
     * against keeps working; file-backed and TCP-server H2 schemes are deliberately excluded. Vendor schemes cover
     * the ones we expect to see in early deployments; {@code jdbc:redshift://} is the dedicated Amazon Redshift
     * scheme, added alongside {@code jdbc:postgresql://} because Redshift's native driver uses its own
     * scheme and is served by {@link RedshiftDialect}. {@code jdbc:redshift:iam:} is the Redshift IAM sub-scheme:
     * it is a SEPARATE allowlist entry because after {@code jdbc:redshift:} the IAM form has
     * {@code iam://...}, not {@code //...}, so it does NOT prefix-match the {@code jdbc:redshift://} entry above and
     * would otherwise be blocked. The IAM auth exchange itself is done by the driver via the AWS SDK credential chain
     * (see {@link JdbcConnectorFactory}); the guard only vets the scheme + host (see {@link #extractHost}). We use a
     * {@link LinkedHashSet} so error messages report the allowlist in a stable, documentation-friendly order.
     */
    static final Set<String> DEFAULT_ALLOWED_SUBPROTOCOLS;

    static {
        LinkedHashSet<String> defaults = new LinkedHashSet<>();
        defaults.add("jdbc:postgresql://");
        defaults.add("jdbc:redshift://");
        defaults.add("jdbc:redshift:iam:");
        defaults.add("jdbc:mysql://");
        defaults.add("jdbc:snowflake://");
        defaults.add("jdbc:sqlserver://");
        defaults.add("jdbc:oracle:thin:");
        defaults.add("jdbc:h2:mem:");
        DEFAULT_ALLOWED_SUBPROTOCOLS = java.util.Collections.unmodifiableSet(defaults);
    }

    /** Decision returned from {@link #evaluate}. {@code allowed=true} permits the URL; otherwise {@code reason} is non-null. */
    record Decision(boolean allowed, String reason) {
        static final Decision ALLOWED = new Decision(true, null);

        static Decision denied(String reason) {
            return new Decision(false, reason);
        }
    }

    private final Set<String> allowedSubprotocols;
    private final boolean allowLoopback;

    SsrfGuard(Collection<String> allowedSubprotocols, boolean allowLoopback) {
        if (allowedSubprotocols == null) {
            throw new IllegalArgumentException("allowedSubprotocols must not be null");
        }
        // Normalise everything to lower-case once so case-insensitive matching is a hash lookup. LinkedHashSet
        // preserves the configured order for predictable log lines.
        LinkedHashSet<String> normalized = new LinkedHashSet<>(allowedSubprotocols.size());
        for (String s : allowedSubprotocols) {
            if (s != null && s.isEmpty() == false) {
                normalized.add(s.toLowerCase(Locale.ROOT));
            }
        }
        this.allowedSubprotocols = Set.copyOf(normalized);
        this.allowLoopback = allowLoopback;
    }

    /**
     * Returns an SSRF guard with the default allowlist and {@code allowLoopback=false}. Used as the
     * production default; tests that need {@code jdbc:h2:mem:} (which has no host so loopback doesn't apply) work
     * out of the box, while tests that want {@code jdbc:h2:tcp://localhost:...} must explicitly enable loopback.
     */
    static SsrfGuard defaultGuard() {
        return new SsrfGuard(DEFAULT_ALLOWED_SUBPROTOCOLS, false);
    }

    /**
     * Evaluates {@code jdbcUrl} against the allowlist and host filters. Returns {@link Decision#ALLOWED} when the
     * URL passes both, or {@link Decision#denied(String)} carrying a short human-readable reason. The caller is
     * responsible for logging at the right level and propagating the decision to {@code canHandle()}.
     */
    Decision evaluate(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return Decision.denied("URL is null or empty");
        }
        String lower = jdbcUrl.toLowerCase(Locale.ROOT);
        String matched = matchSubprotocol(lower);
        if (matched == null) {
            return Decision.denied("subprotocol is not in the allowlist; configured allowlist=" + allowedSubprotocols);
        }
        // jdbc:h2:mem: has no host component at all; the entire rest of the URL is the in-process DB name plus
        // session properties. Skip host parsing for it -- it can never reach the network.
        if (matched.equals("jdbc:h2:mem:")) {
            return Decision.ALLOWED;
        }
        String host = extractHost(jdbcUrl, matched);
        if (host == null || host.isEmpty()) {
            // Subprotocol said it carries TCP; no host means the driver will either throw or open something we
            // can't reason about. Conservative refusal -- never the common case in production.
            return Decision.denied("could not parse a host out of [" + JdbcUrlSanitizer.sanitize(jdbcUrl) + "]");
        }
        return evaluateHost(host);
    }

    private String matchSubprotocol(String lowerUrl) {
        for (String prefix : allowedSubprotocols) {
            if (lowerUrl.startsWith(prefix)) {
                return prefix;
            }
        }
        return null;
    }

    /**
     * Extracts the host portion immediately after the matched subprotocol. Handles two well-known shapes:
     * <ul>
     *   <li>Authority-based ({@code jdbc:vendor://host:port/db}): host runs from end-of-prefix to the first of
     *       {@code :}, {@code /}, {@code ?}, {@code ;}, or end-of-string.</li>
     *   <li>Oracle-thin ({@code jdbc:oracle:thin:@host:port:sid}): the {@code @} precedes the host. We accept
     *       either form because Oracle accepts both.</li>
     *   <li>Redshift IAM ({@code jdbc:redshift:iam://<host-or-clusterid>:<port-or-region>/db}): the allowlist entry
     *       ends at the {@code :} before the authority, so the remainder still carries the leading {@code //}; we
     *       strip it and extract the token up to the first {@code :}. That token is the network host for the
     *       {@code host:port} form (subject to the normal loopback/link-local host filter) or the Redshift
     *       cluster-id for the {@code cluster-id:region} form. In the cluster-id form the {@code :region} suffix is
     *       NOT a TCP port and the real endpoint is AWS-resolved by the driver, so the guard cannot vet the network
     *       target -- it vets only the scheme + the extracted token (a plain cluster-id is neither an IP literal nor
     *       a known-bad name, so it passes the host filter, which is the intended behaviour: don't reject the
     *       cluster-id:region form as an invalid-port/loopback host).</li>
     * </ul>
     * The leading-{@code //} strip is scoped to opaque/{@code :}-terminated prefixes (the two forms above). For an
     * authority-form prefix that already ends in {@code //} (e.g. {@code jdbc:postgresql://}), a leftover leading
     * {@code //} is a MALFORMED authority (e.g. {@code jdbc:postgresql:////host}) and is deliberately NOT repaired, so
     * such URLs stay fail-closed. For other shapes we return {@code null} to let the caller deny conservatively.
     */
    private static String extractHost(String jdbcUrl, String matchedPrefix) {
        String rest = jdbcUrl.substring(matchedPrefix.length());
        if (rest.isEmpty()) {
            return null;
        }
        // Oracle thin: jdbc:oracle:thin:@host:port:sid -- strip the leading '@' if present.
        if (rest.charAt(0) == '@') {
            rest = rest.substring(1);
        }
        // Opaque/':'-terminated allowlist entries (e.g. jdbc:redshift:iam:, jdbc:oracle:thin:) do NOT consume the
        // authority marker in the prefix itself, so after the optional '@' the remainder can still carry a leading
        // '//' (jdbc:redshift:iam://host, or Oracle EZConnect jdbc:oracle:thin:@//host:port/service). Strip it there
        // so host extraction sees the authority directly. Crucially, this strip is scoped to prefixes that did NOT
        // already end in '//': for an authority-form prefix (e.g. jdbc:postgresql://) a leftover leading '//' means a
        // MALFORMED authority such as jdbc:postgresql:////host, which stays fail-closed (extractHost returns null =>
        // conservative deny) rather than being silently repaired into a checkable host.
        if (matchedPrefix.endsWith("//") == false && rest.startsWith("//")) {
            rest = rest.substring(2);
        }
        if (rest.isEmpty()) {
            return null;
        }
        // Strip any userinfo: "user:pass@host". Last '@' before the first '/' is the userinfo terminator.
        int slash = firstIndexOfAny(rest, "/?;");
        int lookupEnd = slash < 0 ? rest.length() : slash;
        int at = rest.lastIndexOf('@', lookupEnd - 1);
        if (at >= 0 && at < lookupEnd) {
            rest = rest.substring(at + 1);
            slash = firstIndexOfAny(rest, "/?;");
            lookupEnd = slash < 0 ? rest.length() : slash;
        }
        // Host runs to the first of ':' (port), '/', '?', ';'.
        int colon = rest.indexOf(':');
        int hostEnd = colon < 0 ? lookupEnd : Math.min(colon, lookupEnd);
        // IPv6 literal in brackets [::1]: take everything to ']'.
        if (rest.startsWith("[")) {
            int close = rest.indexOf(']');
            if (close > 0) {
                return rest.substring(1, close);
            }
        }
        return hostEnd == 0 ? null : rest.substring(0, hostEnd);
    }

    private static int firstIndexOfAny(String s, String chars) {
        int best = -1;
        for (int i = 0; i < chars.length(); i++) {
            int idx = s.indexOf(chars.charAt(i));
            if (idx >= 0 && (best < 0 || idx < best)) {
                best = idx;
            }
        }
        return best;
    }

    /**
     * Applies host-level filters. We trust {@link InetAddresses#isInetAddress(String)} to recognize IPv4 / IPv6
     * literals without resolving anything; for hostnames we only block the obvious local names. Real DNS-based
     * filtering belongs in a network policy, not here.
     */
    private Decision evaluateHost(String host) {
        if (host.equalsIgnoreCase("localhost") || host.equalsIgnoreCase("localhost.localdomain")) {
            return allowLoopback ? Decision.ALLOWED : Decision.denied("host [" + host + "] is loopback");
        }
        if (InetAddresses.isInetAddress(host)) {
            InetAddress addr = InetAddresses.forString(host);
            if (addr.isLoopbackAddress() && allowLoopback == false) {
                // Covers 127.0.0.0/8 (including ::ffff:127.0.0.0/8 once IPv4-mapped is normalized) and ::1.
                return Decision.denied("host [" + host + "] is loopback");
            }
            if (addr.isLinkLocalAddress()) {
                // Link-local includes 169.254.0.0/16 (cloud metadata) and fe80::/10. Always refused -- there is
                // no legitimate reason for ES to talk to either of these via a JDBC driver.
                return Decision.denied("host [" + host + "] is link-local (e.g. cloud metadata, IPv6 link-local)");
            }
            if (addr.isAnyLocalAddress()) {
                return Decision.denied("host [" + host + "] is wildcard / unspecified");
            }
            if (addr.isMulticastAddress()) {
                return Decision.denied("host [" + host + "] is multicast");
            }
            return Decision.ALLOWED;
        }
        // Non-literal hostname. Block the well-known cloud metadata names; otherwise pass through.
        if (host.equalsIgnoreCase("metadata.google.internal") || host.equalsIgnoreCase("metadata.azure.com")) {
            return Decision.denied("host [" + host + "] is a known cloud metadata endpoint");
        }
        return Decision.ALLOWED;
    }

    /** Visible for tests. */
    Set<String> allowedSubprotocols() {
        return allowedSubprotocols;
    }

    /** Visible for tests. */
    boolean allowLoopback() {
        return allowLoopback;
    }

    /** Returns a debug-friendly summary of the guard configuration. */
    @Override
    public String toString() {
        return "SsrfGuard{allowed=" + allowedSubprotocols + ", allowLoopback=" + allowLoopback + "}";
    }

    /** Helper to construct a guard from a comma-separated list (used by the cluster setting parser). */
    static SsrfGuard parse(String csv, boolean allowLoopback) {
        if (csv == null || csv.isBlank()) {
            return new SsrfGuard(DEFAULT_ALLOWED_SUBPROTOCOLS, allowLoopback);
        }
        String[] parts = csv.split(",");
        // Trim and skip empties so a trailing comma in the setting value isn't fatal.
        LinkedHashSet<String> filtered = new LinkedHashSet<>(parts.length);
        for (String p : parts) {
            String t = p.trim();
            if (t.isEmpty() == false) {
                filtered.add(t);
            }
        }
        return new SsrfGuard(filtered, allowLoopback);
    }

}
