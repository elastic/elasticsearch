/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import java.util.Locale;
import java.util.Set;

/**
 * Redacts credential-bearing portions of JDBC URLs so we can include the URL in log lines and wrapped exception
 * messages without leaking secrets.
 * <p>
 * JDBC URLs carry credentials in four shapes; this class strips/redacts each:
 * <ul>
 *   <li>{@code scheme://user:pass@host} URI userinfo</li>
 *   <li>Oracle-thin {@code jdbc:oracle:thin:user/pass@host:port:sid} -- userinfo precedes the {@code @}</li>
 *   <li>{@code ?key=value&...} query parameters where {@code key} is a credential name</li>
 *   <li>{@code ;key=value;...} property list (SQL Server / Sybase) where {@code key} is a credential name</li>
 * </ul>
 * Non-credential properties (e.g. {@code ssl=true}, {@code applicationName=...}) survive so an operator can still see
 * the connection shape. A whole {@code ?...} query is dropped because we can't be sure how individual vendors parse
 * it (e.g. some treat it as opaque); operators rarely need the query string in error messages.
 */
final class JdbcUrlSanitizer {

    /**
     * Lower-cased property names that must be scrubbed wherever they appear. Besides the usual {@code user}/
     * {@code password} family this also covers the Redshift IAM explicit AWS credentials: the
     * driver property names ({@code AccessKeyID}/{@code SecretAccessKey}/{@code SessionToken}) and the connector
     * config keys ({@code access_key_id}/{@code secret_access_key}/{@code session_token}). Listing them here means
     * (a) the {@code connection_properties} passthrough rejects any attempt to smuggle an AWS credential through the
     * non-secret tuning map (via {@link #credentialKeys()}), and (b) {@code ;key=value} URL properties bearing these
     * keys are redacted. The loose free-form redaction in {@link #sanitizeMessage} is handled by
     * {@link #CREDENTIAL_KV_PATTERN} below, which must stay in sync with this set.
     */
    private static final Set<String> CREDENTIAL_KEYS = Set.of(
        "user",
        "username",
        "password",
        "pwd",
        "passwd",
        "auth",
        "accesskeyid",
        "secretaccesskey",
        "sessiontoken",
        "access_key_id",
        "secret_access_key",
        "session_token"
    );

    /**
     * The lower-cased credential property names this sanitizer scrubs. Exposed so the {@code connection_properties}
     * passthrough ({@link JdbcConnectionProperties}) can reject any attempt to smuggle a credential through the
     * non-secret tuning map, keeping a single source of truth for "what is a credential key".
     */
    static Set<String> credentialKeys() {
        return CREDENTIAL_KEYS;
    }

    private static final String REDACTED = "REDACTED";

    /**
     * Matches {@code key=value} pairs anywhere in a string where {@code key} is a credential name (case-insensitive)
     * and {@code value} runs until the next delimiter ({@code &}, {@code ;}, whitespace, quote, or end-of-string).
     * Used to scrub free-form error messages that drivers compose from properties.
     */
    private static final java.util.regex.Pattern CREDENTIAL_KV_PATTERN = java.util.regex.Pattern.compile(
        "(?i)\\b(user|username|password|pwd|passwd|auth|accesskeyid|secretaccesskey|sessiontoken"
            + "|access_key_id|secret_access_key|session_token)\\s*=\\s*[^;&\\s\"'`]*"
    );

    /**
     * Matches a {@code jdbc:} URL-ish token inside free-form text. Stops at whitespace, quote characters, and
     * common delimiters that almost never appear unescaped inside a JDBC URL. The match is then handed to
     * {@link #sanitize(String)} so we get the full userinfo+query+property redaction we apply to URLs we format
     * ourselves -- crucially, that path locates userinfo by scanning for {@code @}, NOT by pattern-matching
     * userinfo characters, so it handles passwords with {@code %}, {@code !}, {@code $}, {@code ~}, {@code =}, etc.
     */
    private static final java.util.regex.Pattern JDBC_URL_TOKEN = java.util.regex.Pattern.compile("jdbc:[^\\s\"'`,)>\\]]+");

    /**
     * Fallback userinfo matcher for non-{@code jdbc:} prose ({@code "connect to alice:s3cret@host failed"}). Permits
     * almost any character except {@code @}, whitespace, quote characters, and {@code /} (the latter would misread
     * paths as userinfo). Length-bounded so a pathological message can't trigger super-linear backtracking.
     */
    private static final java.util.regex.Pattern USERINFO_PATTERN = java.util.regex.Pattern.compile(
        "(?<![\\w.+\\-/])([^@\\s\"'`/]{1,256}:[^@\\s\"'`/]{1,256})@"
    );

    private JdbcUrlSanitizer() {}

    static String sanitize(String url) {
        if (url == null || url.isEmpty()) {
            return url;
        }
        String result = url;
        // 1. Drop ?query string entirely. Vendor-specific parsing makes per-key redaction unreliable.
        int q = result.indexOf('?');
        if (q >= 0) {
            result = result.substring(0, q);
        }
        // 2. Redact userinfo: any '@' preceded by something that looks like credentials. We only redact the FIRST
        // '@' so legitimate '@' in dbnames (rare but possible after the credential block) is left alone.
        int at = result.indexOf('@');
        if (at > 0) {
            // Anchor: userinfo lives between scheme-prefix and '@'. Find the closest delimiter (':' or '/' followed
            // by non-slash) before the '@' that marks where userinfo started. Conservative: rewrite from after the
            // last '/' or ':' (whichever is later) before the '@'.
            int start = lastSchemeAnchorBefore(result, at);
            result = result.substring(0, start) + REDACTED + result.substring(at);
        }
        // 3. Redact ;key=value;... property pairs whose key is a credential name. Properties live after the last ';'
        // block; keys are case-insensitive in every vendor that uses this form.
        result = redactSemicolonProperties(result);
        return result;
    }

    /**
     * Redacts credential-bearing substrings inside a free-form text message (typically a {@link java.sql.SQLException}
     * message produced by a JDBC driver). Drivers frequently embed the connection URL or named credential properties
     * verbatim ("Connection to alice:s3cret@host failed", "password=hunter2"). This method scrubs those patterns
     * without otherwise changing the message, so log lines stay legible but never leak secrets.
     * <p>
     * Unlike {@link #sanitize(String)} this is conservative: we don't drop trailing query strings (the message may
     * legitimately contain a {@code ?} that isn't a URL query), only redact credential key=value pairs and userinfo
     * groups. {@code null} input returns {@code null}.
     */
    static String sanitizeMessage(String message) {
        if (message == null || message.isEmpty()) {
            return message;
        }
        // 1. JDBC URL tokens go through the URL sanitizer wholesale -- it locates userinfo by '@' position, not by
        // character class, so it handles ANY password characters (%, !, $, ~, =, ...). It also strips the query
        // string and redacts ;key=value credential properties. This is the main defense against driver messages
        // that echo the connection URL verbatim.
        StringBuilder sb = null;
        java.util.regex.Matcher urlMatcher = JDBC_URL_TOKEN.matcher(message);
        while (urlMatcher.find()) {
            if (sb == null) {
                sb = new StringBuilder(message.length() + 16);
            }
            urlMatcher.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(sanitize(urlMatcher.group())));
        }
        String result;
        if (sb != null) {
            urlMatcher.appendTail(sb);
            result = sb.toString();
        } else {
            result = message;
        }
        // 2. Free-form userinfo prose without a jdbc: prefix (driver-internal exception text often takes this form).
        result = USERINFO_PATTERN.matcher(result).replaceAll(REDACTED + "@");
        // 3. Loose key=value credential pairs anywhere in the text.
        result = CREDENTIAL_KV_PATTERN.matcher(result).replaceAll(matchResult -> matchResult.group(1) + "=" + REDACTED);
        return result;
    }

    /**
     * Returns a sanitized copy of an SQL exception suitable for use as a wrapper-exception cause. SQLState and
     * vendor error code are preserved (they never carry credentials); message and cause-chain messages are scrubbed
     * via {@link #sanitizeMessage(String)}. The cause chain is walked iteratively (depth-bounded, cycle-aware) so a
     * pathological driver can't make us spin.
     * <p>
     * We deliberately do NOT preserve the original {@link Throwable} subclasses in the cause chain because the
     * original exception's {@code toString()} (used by stack traces and many log appenders) prints the unsanitized
     * message. Instead we replace each link with a fresh exception whose message is sanitized.
     */
    static java.sql.SQLException sanitizeException(java.sql.SQLException original) {
        if (original == null) {
            return null;
        }
        java.sql.SQLException sanitized = new java.sql.SQLException(
            sanitizeMessage(original.getMessage()),
            original.getSQLState(),
            original.getErrorCode()
        );
        sanitized.setStackTrace(original.getStackTrace());
        Throwable cause = original.getCause();
        java.util.IdentityHashMap<Throwable, Boolean> seen = new java.util.IdentityHashMap<>();
        Throwable copyParent = sanitized;
        int depth = 0;
        while (cause != null && depth++ < 16 && seen.put(cause, Boolean.TRUE) == null) {
            Throwable copyCause;
            if (cause instanceof java.sql.SQLException sqlCause) {
                copyCause = new java.sql.SQLException(
                    sanitizeMessage(sqlCause.getMessage()),
                    sqlCause.getSQLState(),
                    sqlCause.getErrorCode()
                );
            } else {
                copyCause = new RuntimeException(cause.getClass().getName() + ": " + sanitizeMessage(cause.getMessage()));
            }
            copyCause.setStackTrace(cause.getStackTrace());
            copyParent.initCause(copyCause);
            copyParent = copyCause;
            cause = cause.getCause();
        }
        return sanitized;
    }

    /**
     * Returns the index where userinfo begins inside {@code url} given that {@code at} is the index of the
     * credential-terminating {@code @}. We rewind through the userinfo characters until we hit the scheme separator.
     * For {@code jdbc:postgresql://user:pass@host} the userinfo starts right after {@code //}; for
     * {@code jdbc:oracle:thin:user/pass@host} the userinfo starts after the last {@code :} before the {@code @}.
     */
    private static int lastSchemeAnchorBefore(String url, int at) {
        // Look for "//" first -- that's the URI authority marker. If present and before '@', userinfo starts after it.
        int authority = url.lastIndexOf("//", at);
        if (authority >= 0 && authority + 2 < at) {
            return authority + 2;
        }
        // Otherwise fall back to the last ':' before '@' (Oracle-thin form). Userinfo starts after that ':'.
        int colon = url.lastIndexOf(':', at);
        if (colon >= 0) {
            return colon + 1;
        }
        // Pathological input: just rewrite from the start.
        return 0;
    }

    /**
     * Rewrites {@code ;key=value} pairs whose key matches a credential name with {@code ;key=REDACTED}. Other pairs
     * are preserved verbatim. The first {@code ;} (and everything before it) is preserved as-is.
     */
    private static String redactSemicolonProperties(String url) {
        int firstSemi = url.indexOf(';');
        if (firstSemi < 0) {
            return url;
        }
        String head = url.substring(0, firstSemi);
        String tail = url.substring(firstSemi); // includes leading ';'
        StringBuilder sb = new StringBuilder(head.length() + tail.length() + 16);
        sb.append(head);
        // Walk pairs separated by ';'.
        int idx = 0;
        while (idx < tail.length()) {
            // Skip the leading ';'.
            if (tail.charAt(idx) == ';') {
                sb.append(';');
                idx++;
            }
            int next = tail.indexOf(';', idx);
            String pair = next < 0 ? tail.substring(idx) : tail.substring(idx, next);
            int eq = pair.indexOf('=');
            if (eq > 0) {
                String key = pair.substring(0, eq);
                if (CREDENTIAL_KEYS.contains(key.toLowerCase(Locale.ROOT))) {
                    sb.append(key).append('=').append(REDACTED);
                } else {
                    sb.append(pair);
                }
            } else {
                sb.append(pair);
            }
            if (next < 0) {
                break;
            }
            idx = next;
        }
        return sb.toString();
    }
}
