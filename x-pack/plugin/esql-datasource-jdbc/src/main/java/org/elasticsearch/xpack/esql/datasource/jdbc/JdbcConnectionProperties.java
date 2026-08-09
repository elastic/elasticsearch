/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Parses and <b>allowlist-filters</b> the {@code connection_properties} passthrough.
 * <p>
 * <b>Why.</b> Postgres and Postgres-compatible stores share Postgres' SQL dialect but differ
 * in <em>connection</em> nuances — {@code sslmode}, SNI/{@code options=endpoint=...}, application name, timeouts. Before
 * this class the connector could only set {@code user}/{@code password}; every other driver property had to be crammed
 * into the JDBC URL query string. {@code connection_properties} makes a curated, non-secret set of tuning properties
 * first-class without per-store code.
 * <p>
 * <b>Syntax.</b> The ES|QL {@code WITH} options carrier folds every
 * option value to a scalar {@link org.elasticsearch.xpack.esql.core.expression.Literal} — nested maps are rejected by
 * the parser. So the passthrough rides a single scalar string:
 * <pre>{@code WITH ("connection_properties"="sslmode=require;ApplicationName=es;options=endpoint=ep-x")}</pre>
 * Pairs are {@code ;}-separated; each pair is split on the <em>first</em> {@code =} so a value may itself contain
 * {@code =} (needed for pgjdbc {@code options=endpoint=...}). Keys are trimmed and matched case-insensitively; the
 * <b>canonical</b> driver-property casing is what lands in the {@link Properties}.
 * <p>
 * <b>Value limitation ({@code ;} is the delimiter).</b> Because {@code ;} separates pairs, a property VALUE
 * cannot itself contain a {@code ;}: the text after a {@code ;} is parsed as the next pair. A value that embeds
 * {@code ;} therefore fails cleanly with an {@link IllegalArgumentException} (the trailing fragment is either an
 * unlabeled segment -&gt; "malformed" or an unrecognized key -&gt; "not permitted") rather than being silently
 * truncated. A driver property that genuinely needs a {@code ;} in its value is not expressible through this
 * passthrough and must be placed directly in the JDBC URL instead.
 * <p>
 * <b>Security model — default deny.</b> Only keys on {@link #ALLOWED} pass; everything else is rejected. Footguns that
 * can load classes, touch the filesystem, or re-point/relax the connection ({@link #BLOCKED}) are rejected with a
 * dedicated message even though default-deny would already reject them. Credential keys
 * ({@link JdbcUrlSanitizer#credentialKeys()}) are rejected outright: secrets must use the typed
 * {@code user}/{@code password} SecureString channel, never this map. Error messages name only the offending KEY,
 * never the value.
 * <p>
 * A dynamic {@code esql.jdbc.connection_properties.allowed} cluster setting to extend {@link #ALLOWED} at runtime is a
 * possible future extension; for now the allowlist is a curated static set.
 */
final class JdbcConnectionProperties {

    /** The single scalar config/WITH key carrying the passthrough. */
    static final String CONFIG_KEY = "connection_properties";

    /**
     * Default-deny allowlist of safe, NON-secret tuning properties, keyed by the lower-cased input name and mapped to
     * the driver's canonical property casing (what is actually written into {@link Properties}). Curated for the
     * pgjdbc / Postgres-family surface; none of these can re-point the connection host (that lives in the URL and is
     * governed by {@link SsrfGuard}), load a class, or read/write a file:
     * <ul>
     *   <li>{@code sslmode}, {@code ssl} — TLS negotiation mode (Neon/Aurora require {@code sslmode=require}).</li>
     *   <li>{@code ApplicationName} — server-side session label for observability.</li>
     *   <li>{@code connectTimeout}, {@code socketTimeout}, {@code loginTimeout} — timeouts (serverless cold starts).</li>
     *   <li>{@code tcpKeepAlive} — keepalive toggle for long-lived pooled connections.</li>
     *   <li>{@code options} — server startup GUCs, incl. Neon SNI routing {@code options=endpoint=<id>}. Applied to the
     *       already-connected server; cannot change the connection target.</li>
     *   <li>{@code currentSchema} — default search path.</li>
     *   <li>{@code readOnly} — read-only session hint (our scan surface is read-only anyway).</li>
     *   <li>{@code assumeMinServerVersion} — protocol tuning.</li>
     *   <li>{@code targetServerType} — primary/secondary selection among hosts ALREADY in the URL (adds no host).</li>
     *   <li>{@code preferQueryMode} — simple/extended protocol selection.</li>
     * </ul>
     * <b>Redshift IAM non-secret params.</b> IAM authentication needs a handful of NON-secret
     * driver knobs that select the temporary DB user / cluster / region and toggle IAM mode; these are safe to carry
     * through this passthrough because none of them is a secret and none can re-point the connection (the network
     * endpoint is still the URL host / AWS-resolved cluster, governed by {@link SsrfGuard}):
     * <ul>
     *   <li>{@code DbUser} — the temporary DB user the driver requests credentials for.</li>
     *   <li>{@code ClusterID} — the Redshift cluster identifier (used with the {@code cluster-id:region} URL form).</li>
     *   <li>{@code Region} — the AWS region for the {@code GetClusterCredentials} call.</li>
     *   <li>{@code AutoCreate} — create the DB user on first login if absent.</li>
     *   <li>{@code DbGroups} — DB groups to add the temporary user to.</li>
     *   <li>{@code IAM} — toggles IAM authentication on ({@code IAM=1}).</li>
     * </ul>
     * The SECRET AWS credentials ({@code AccessKeyID}/{@code SecretAccessKey}/{@code SessionToken}) are deliberately
     * NOT here: they ride the typed SecureString channel (mirroring {@code user}/{@code password}) and this map rejects
     * them as credential keys (see {@link JdbcUrlSanitizer#credentialKeys()}). {@code Plugin_Name} (arbitrary-class
     * load → RCE) is NOT allowlisted and is in the explicit {@link #BLOCKED} set, matching
     * {@code authenticationPluginClassName}.
     */
    private static final Map<String, String> ALLOWED = Map.ofEntries(
        Map.entry("sslmode", "sslmode"),
        Map.entry("ssl", "ssl"),
        Map.entry("applicationname", "ApplicationName"),
        Map.entry("connecttimeout", "connectTimeout"),
        Map.entry("sockettimeout", "socketTimeout"),
        Map.entry("logintimeout", "loginTimeout"),
        Map.entry("tcpkeepalive", "tcpKeepAlive"),
        Map.entry("options", "options"),
        Map.entry("currentschema", "currentSchema"),
        Map.entry("readonly", "readOnly"),
        Map.entry("assumeminserverversion", "assumeMinServerVersion"),
        Map.entry("targetservertype", "targetServerType"),
        Map.entry("preferquerymode", "preferQueryMode"),
        // Redshift IAM non-secret params. Canonical driver casing on the right.
        Map.entry("dbuser", "DbUser"),
        Map.entry("clusterid", "ClusterID"),
        Map.entry("region", "Region"),
        Map.entry("autocreate", "AutoCreate"),
        Map.entry("dbgroups", "DbGroups"),
        Map.entry("iam", "IAM")
    );

    /**
     * Explicitly blocked footguns (lower-cased). Redundant with default-deny, but kept explicit so the operator gets a
     * pointed error and so the protection survives any future widening of {@link #ALLOWED}. Each of these can execute
     * code, read/write local files, or re-point / silently relax the connection:
     * <ul>
     *   <li>{@code socketFactory}/{@code socketFactoryArg} — loads an arbitrary {@code SocketFactory} class → RCE and
     *       full connection re-point (SSRF bypass).</li>
     *   <li>{@code sslfactory}/{@code sslfactoryarg}/{@code sslhostnameverifier} — arbitrary class → RCE / disables
     *       certificate verification.</li>
     *   <li>{@code loggerFile}/{@code loggerLevel} — writes driver logs to an arbitrary filesystem path.</li>
     *   <li>{@code sslcert}/{@code sslkey}/{@code sslrootcert}/{@code sslpassword} — local file reads and a secret.</li>
     *   <li>{@code authenticationPluginClassName} — loads an arbitrary auth-plugin class → RCE.</li>
     *   <li>{@code Plugin_Name} — the Redshift/pgjdbc credential-provider plugin class selector; loads an arbitrary
     *       class → RCE, exactly like {@code authenticationPluginClassName}. Default-deny already rejects it, but it is
     *       listed here explicitly so it survives a future widening of {@link #ALLOWED} and yields the pointed
     *       footgun-rejection message.</li>
     *   <li>{@code jaasApplicationName}/{@code gsslib}/{@code sspiServiceClass} — alternative auth / native library
     *       selection footguns.</li>
     * </ul>
     */
    static final Set<String> BLOCKED = Set.of(
        "socketfactory",
        "socketfactoryarg",
        "sslfactory",
        "sslfactoryarg",
        "sslhostnameverifier",
        "loggerfile",
        "loggerlevel",
        "sslcert",
        "sslkey",
        "sslrootcert",
        "sslpassword",
        "authenticationpluginclassname",
        "plugin_name",
        "jaasapplicationname",
        "gsslib",
        "sspiserviceclass"
    );

    private JdbcConnectionProperties() {}

    /**
     * Rejects a JDBC URL that carries any {@link #BLOCKED} driver property in the URL itself (e.g.
     * {@code ?socketFactory=...}, {@code ;Plugin_Name=...}, {@code &sslfactory=...}). The {@code connection_properties}
     * passthrough already blocks these keys, but the same footguns can ride the URL query string / property list
     * straight into {@link java.sql.Driver#connect}, so this guard closes that gap using the SAME {@link #BLOCKED}
     * set (single source of truth).
     * <p>
     * A key is treated as a property assignment when a {@link #BLOCKED} token (case-insensitive) is immediately
     * followed by optional spaces and an {@code =}, AND is preceded (skipping optional spaces) by one of the property
     * delimiters {@code ?}, {@code &}, or {@code ;}. The delimiter requirement avoids matching a blocked token that is
     * merely a suffix of a longer, harmless property name. On a match this throws {@link IllegalArgumentException}
     * naming only the offending KEY (never the value), in the same style as the {@code connection_properties} path.
     */
    static void assertUrlHasNoBlockedProperties(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            return;
        }
        String lower = jdbcUrl.toLowerCase(Locale.ROOT);
        for (String blocked : BLOCKED) {
            int from = 0;
            int idx;
            while ((idx = lower.indexOf(blocked, from)) >= 0) {
                from = idx + blocked.length();
                // The token must be a property assignment: optional spaces then '=' immediately after the key.
                int after = idx + blocked.length();
                while (after < lower.length() && (lower.charAt(after) == ' ' || lower.charAt(after) == '\t')) {
                    after++;
                }
                if (after >= lower.length() || lower.charAt(after) != '=') {
                    continue;
                }
                // The token must be preceded (skipping optional spaces) by a property delimiter, so a blocked key
                // that is only a SUFFIX of a longer harmless property name (e.g. mysocketFactory) is not flagged.
                int before = idx - 1;
                while (before >= 0 && (lower.charAt(before) == ' ' || lower.charAt(before) == '\t')) {
                    before--;
                }
                if (before < 0) {
                    continue;
                }
                char delimiter = lower.charAt(before);
                if (delimiter == '?' || delimiter == '&' || delimiter == ';') {
                    // Report the KEY using the URL's original casing; never echo the value.
                    String key = jdbcUrl.substring(idx, idx + blocked.length());
                    throw new IllegalArgumentException(
                        "JDBC URL property ["
                            + key
                            + "] is blocked: it can load classes, access the filesystem, or re-point/relax the connection "
                            + "and is never permitted"
                    );
                }
            }
        }
    }

    /**
     * Parses and allowlist-filters the raw {@code connection_properties} string, rejecting an explicit TLS-disable
     * (see {@link #parse(String, boolean)} with {@code allowPlaintext=false}). Used by tests and the pool-key
     * derivation, where the node-level {@code allow_plaintext} flag is not threaded and the safe default applies.
     */
    static Map<String, String> parse(String raw) {
        return parse(raw, false);
    }

    /**
     * Parses and allowlist-filters the raw {@code connection_properties} string into an ordered map of
     * {@code canonical-key → value}. Returns an empty map for {@code null}/blank input. Throws
     * {@link IllegalArgumentException} — naming only the offending KEY — when a pair is malformed, or a key is a
     * credential, is blocked, or is not on the allowlist.
     * <p>
     * <b>TLS-disable policy.</b> {@code sslmode} and {@code ssl} stay allowlisted (TLS modes like
     * {@code require}/{@code verify-full} are the common, safe case), but an EXPLICIT disable —
     * {@code sslmode=disable} or {@code ssl=false} (case-insensitive) — puts credentials on the wire in cleartext
     * and is rejected unless {@code allowPlaintext} is {@code true} (the {@code esql.jdbc.allow_plaintext} node
     * setting). Opportunistic modes ({@code sslmode=prefer}/{@code allow}) are NOT rejected.
     */
    static Map<String, String> parse(String raw, boolean allowPlaintext) {
        Map<String, String> result = new LinkedHashMap<>();
        if (raw == null) {
            return result;
        }
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) {
            return result;
        }
        // -1 limit keeps trailing empty segments so a stray ';' is a no-op rather than silently swallowing input.
        for (String segment : trimmed.split(";", -1)) {
            String pair = segment.trim();
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq < 0) {
                // Do not echo the segment: it could be an unlabeled secret. Point at the required shape instead.
                throw new IllegalArgumentException("malformed connection_properties entry; expected ';'-separated key=value pairs");
            }
            String key = pair.substring(0, eq).trim();
            String value = pair.substring(eq + 1).trim();
            if (key.isEmpty()) {
                throw new IllegalArgumentException("connection_properties contains an entry with an empty key");
            }
            String lower = key.toLowerCase(Locale.ROOT);
            if (JdbcUrlSanitizer.credentialKeys().contains(lower)) {
                throw new IllegalArgumentException(
                    "connection property ["
                        + key
                        + "] is a credential and must be supplied via the typed user/password config keys, "
                        + "not connection_properties"
                );
            }
            if (BLOCKED.contains(lower)) {
                throw new IllegalArgumentException(
                    "connection property ["
                        + key
                        + "] is blocked: it can load classes, access the filesystem, or re-point/relax the connection "
                        + "and is never permitted"
                );
            }
            String canonical = ALLOWED.get(lower);
            if (canonical == null) {
                throw new IllegalArgumentException(
                    "connection property [" + key + "] is not permitted; allowed non-secret tuning properties are " + allowedNames()
                );
            }
            result.put(canonical, value);
        }
        if (allowPlaintext == false) {
            rejectExplicitTlsDisable(result);
        }
        return result;
    }

    /**
     * Rejects an explicit TLS-disable in the already-parsed map: {@code sslmode=disable} or {@code ssl=false}
     * (case-insensitive). These put credentials on the wire in cleartext and are only permitted when the operator
     * has set {@code esql.jdbc.allow_plaintext=true}. Opportunistic modes ({@code prefer}/{@code allow}) are left
     * alone. The message names only the KEY and the rejected mode keyword (neither is a secret).
     */
    private static void rejectExplicitTlsDisable(Map<String, String> parsed) {
        String sslmode = parsed.get("sslmode");
        if (sslmode != null && sslmode.equalsIgnoreCase("disable")) {
            throw new IllegalArgumentException(
                "connection property [sslmode]=disable turns off TLS and would send credentials in cleartext; "
                    + "set esql.jdbc.allow_plaintext=true to permit it"
            );
        }
        String ssl = parsed.get("ssl");
        if (ssl != null && ssl.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException(
                "connection property [ssl]=false turns off TLS and would send credentials in cleartext; "
                    + "set esql.jdbc.allow_plaintext=true to permit it"
            );
        }
    }

    /**
     * Applies the filtered {@code connectionProperties} into {@code props}, <b>never overwriting an existing entry</b>.
     * Since {@link #parse} rejects credential keys, the filtered keys can never be {@code user}/{@code password}; the
     * no-overwrite rule is a defensive guarantee that the typed credentials always win.
     */
    static void applyTo(Properties props, Map<String, String> connectionProperties) {
        if (connectionProperties == null || connectionProperties.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : connectionProperties.entrySet()) {
            if (props.containsKey(entry.getKey())) {
                continue;
            }
            props.setProperty(entry.getKey(), entry.getValue());
        }
    }

    /** Sorted canonical allowlist names, for use in error messages (never contains a secret). */
    private static String allowedNames() {
        return new TreeSet<>(ALLOWED.values()).toString();
    }
}
