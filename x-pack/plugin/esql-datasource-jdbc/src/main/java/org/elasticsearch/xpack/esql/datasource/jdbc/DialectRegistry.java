/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import java.util.Locale;
import java.util.Map;

/**
 * Resolves a {@link JdbcDialect} from a JDBC URL by matching a lowercased subprotocol prefix. Immutable after
 * construction; the {@link #defaultRegistry()} singleton holds the shipped mapping.
 * <p>
 * Matching is a case-insensitive {@code startsWith} on the URL (so {@code JDBC:POSTGRESQL://…} resolves the same as
 * {@code jdbc:postgresql://…}). The prefix map is tiny (a couple of vendor entries), so a scan over its entries is
 * effectively O(1); any URL that matches no prefix -- including {@code jdbc:h2:…} -- resolves to the
 * {@link GenericDialect}. The seeded mapping is:
 * <ul>
 *   <li>{@code jdbc:postgresql://} → {@link PostgresDialect#INSTANCE}</li>
 *   <li>{@code jdbc:redshift://} → {@link RedshiftDialect#INSTANCE}: the dedicated Amazon Redshift scheme,
 *       served by the thin {@link RedshiftDialect} (drops the Postgres session {@code statement_timeout}, keeps the
 *       UTC pin, refuses {@code SUPER}/{@code VARBYTE}/{@code GEOMETRY}/{@code GEOGRAPHY}). A Redshift cluster reached
 *       through plain pgjdbc + {@code jdbc:postgresql://} instead resolves to {@link PostgresDialect}; the dedicated
 *       prefix is the robust way to get Redshift-specific handling (product-name auto-detection cannot see it — see
 *       {@link JdbcConnector}).</li>
 *   <li>{@code jdbc:redshift:iam://} → {@link RedshiftDialect#INSTANCE}: the Redshift IAM auth sub-scheme.
 *       The SQL dialect and type mapping are identical to plain Redshift (the difference is purely how the driver
 *       authenticates — via the AWS SDK credential chain), so it maps to the same dialect. A separate entry is
 *       required because {@code jdbc:redshift:iam://…} does not {@code startsWith} {@code jdbc:redshift://}.</li>
 *   <li>everything else (default) → {@link GenericDialect#INSTANCE}</li>
 * </ul>
 */
public final class DialectRegistry {

    private static final DialectRegistry DEFAULT = new DialectRegistry(
        Map.of(
            "jdbc:postgresql://",
            PostgresDialect.INSTANCE,
            "jdbc:redshift://",
            RedshiftDialect.INSTANCE,
            // The Redshift IAM sub-scheme routes to the SAME RedshiftDialect. It needs its own
            // prefix because jdbc:redshift:iam://... does not startsWith jdbc:redshift:// (after jdbc:redshift: comes
            // iam://, not //). The IAM auth exchange is the driver's job; the dialect (SQL/type handling) is identical.
            "jdbc:redshift:iam://",
            RedshiftDialect.INSTANCE
        ),
        GenericDialect.INSTANCE
    );

    private final Map<String, JdbcDialect> byPrefix;
    private final JdbcDialect defaultDialect;

    /**
     * @param byPrefix       subprotocol prefixes → dialect; keys MUST be lowercase (validated) so {@link #resolve}'s
     *                       case-insensitive match is correct. A defensive immutable copy is taken.
     * @param defaultDialect returned when no prefix matches; must not be null.
     */
    DialectRegistry(Map<String, JdbcDialect> byPrefix, JdbcDialect defaultDialect) {
        if (byPrefix == null) {
            throw new IllegalArgumentException("byPrefix must not be null");
        }
        if (defaultDialect == null) {
            throw new IllegalArgumentException("defaultDialect must not be null");
        }
        for (Map.Entry<String, JdbcDialect> e : byPrefix.entrySet()) {
            String key = e.getKey();
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("dialect prefix must not be null or empty");
            }
            if (key.equals(key.toLowerCase(Locale.ROOT)) == false) {
                throw new IllegalArgumentException("dialect prefix must be lowercase for case-insensitive matching: [" + key + "]");
            }
            if (e.getValue() == null) {
                throw new IllegalArgumentException("dialect for prefix [" + key + "] must not be null");
            }
        }
        this.byPrefix = Map.copyOf(byPrefix);
        this.defaultDialect = defaultDialect;
    }

    /** The shipped registry: {@code jdbc:postgresql://} → Postgres, {@code jdbc:redshift://} → Redshift, everything else → generic. */
    public static DialectRegistry defaultRegistry() {
        return DEFAULT;
    }

    /**
     * Returns the dialect for {@code url}, matching a subprotocol prefix case-insensitively. A {@code null} URL, or one
     * matching no prefix, yields the {@link #defaultDialect}.
     */
    public JdbcDialect resolve(String url) {
        if (url == null) {
            return defaultDialect;
        }
        String lower = url.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, JdbcDialect> e : byPrefix.entrySet()) {
            if (lower.startsWith(e.getKey())) {
                return e.getValue();
            }
        }
        return defaultDialect;
    }
}
