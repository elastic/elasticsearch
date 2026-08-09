/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit coverage for {@link DialectRegistry}: subprotocol dispatch, case-insensitivity, default fallback (including
 * {@code jdbc:h2:} and {@code null}), and the immutability / lowercase-key invariants the constructor enforces.
 */
public class DialectRegistryTests extends ESTestCase {

    private final DialectRegistry registry = DialectRegistry.defaultRegistry();

    public void testPostgresUrlResolvesToPostgresDialect() {
        assertThat(registry.resolve("jdbc:postgresql://host:5432/db"), sameInstance(PostgresDialect.INSTANCE));
        assertThat(registry.resolve("jdbc:postgresql://host:5432/db?sslmode=require"), sameInstance(PostgresDialect.INSTANCE));
    }

    public void testPgWireStoresReusePostgresDialectViaPostgresqlPrefix() {
        // Locks the Stage-3 design decision: Postgres and Postgres-compatible (pg-wire) stores
        // connect via the shared jdbc:postgresql:// scheme and MUST resolve to the single PostgresDialect rather than
        // proliferating one bespoke dialect per store. These URLs differ only in host/query-string (endpoint routing,
        // sslmode, options=endpoint, pooling) yet all share Postgres SQL + type mapping, so they reuse PostgresDialect.
        assertThat(
            "Neon pooled endpoint reuses PostgresDialect",
            registry.resolve("jdbc:postgresql://ep-cool-name.us-east-2.aws.neon.tech/db?sslmode=require&options=endpoint%3Dep-cool-name"),
            sameInstance(PostgresDialect.INSTANCE)
        );
        assertThat(
            "Aurora PostgreSQL reuses PostgresDialect",
            registry.resolve("jdbc:postgresql://cluster.cluster-abc.us-east-1.rds.amazonaws.com:5432/db"),
            sameInstance(PostgresDialect.INSTANCE)
        );
        assertThat(
            "a pg-wire store over the postgresql scheme reuses PostgresDialect",
            registry.resolve("jdbc:postgresql://pgwire-host:26257/defaultdb?sslmode=verify-full"),
            sameInstance(PostgresDialect.INSTANCE)
        );
    }

    public void testRedshiftUrlResolvesToRedshiftDialect() {
        // The dedicated jdbc:redshift:// scheme resolves to RedshiftDialect (not PostgresDialect), which is
        // the robust way to get Redshift-specific handling (dropped statement_timeout, refused SUPER, etc.).
        assertThat(
            registry.resolve("jdbc:redshift://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/db"),
            sameInstance(RedshiftDialect.INSTANCE)
        );
        assertThat(
            registry.resolve("jdbc:redshift://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/db?ssl=true"),
            sameInstance(RedshiftDialect.INSTANCE)
        );
    }

    public void testRedshiftAndPostgresAreDistinctDialects() {
        // The two pg-family schemes must not collide: Redshift gets RedshiftDialect, plain Postgres gets PostgresDialect.
        assertThat(registry.resolve("jdbc:redshift://h:5439/db"), sameInstance(RedshiftDialect.INSTANCE));
        assertThat(registry.resolve("jdbc:postgresql://h:5432/db"), sameInstance(PostgresDialect.INSTANCE));
        assertNotSame(registry.resolve("jdbc:redshift://h/db"), registry.resolve("jdbc:postgresql://h/db"));
    }

    public void testRedshiftResolveIsCaseInsensitive() {
        assertThat(registry.resolve("JDBC:REDSHIFT://Host:5439/Db"), sameInstance(RedshiftDialect.INSTANCE));
    }

    public void testRedshiftIamUrlResolvesToRedshiftDialect() {
        // The Redshift IAM sub-scheme uses the SAME RedshiftDialect (auth differs, SQL/type
        // mapping is identical). A dedicated prefix is required because jdbc:redshift:iam://... does not
        // startsWith jdbc:redshift://.
        assertThat(
            registry.resolve("jdbc:redshift:iam://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/db"),
            sameInstance(RedshiftDialect.INSTANCE)
        );
        assertThat(registry.resolve("jdbc:redshift:iam://my-cluster:us-east-1/db"), sameInstance(RedshiftDialect.INSTANCE));
    }

    public void testRedshiftIamResolveIsCaseInsensitive() {
        assertThat(registry.resolve("JDBC:REDSHIFT:IAM://Host:5439/Db"), sameInstance(RedshiftDialect.INSTANCE));
    }

    public void testH2UrlResolvesToGenericDialect() {
        assertThat(registry.resolve("jdbc:h2:mem:test"), sameInstance(GenericDialect.INSTANCE));
        assertThat(registry.resolve("jdbc:h2:file:/tmp/x"), sameInstance(GenericDialect.INSTANCE));
    }

    public void testUnknownPrefixResolvesToGenericDialect() {
        assertThat(registry.resolve("jdbc:mysql://host/db"), sameInstance(GenericDialect.INSTANCE));
        assertThat(registry.resolve("jdbc:oracle:thin:@host:1521:sid"), sameInstance(GenericDialect.INSTANCE));
        assertThat(registry.resolve("not-a-jdbc-url"), sameInstance(GenericDialect.INSTANCE));
    }

    public void testNullResolvesToDefault() {
        assertThat(registry.resolve(null), sameInstance(GenericDialect.INSTANCE));
    }

    public void testResolveIsCaseInsensitive() {
        assertThat(registry.resolve("JDBC:POSTGRESQL://host:5432/db"), sameInstance(PostgresDialect.INSTANCE));
        assertThat(registry.resolve("Jdbc:PostgreSQL://Host/Db"), sameInstance(PostgresDialect.INSTANCE));
        assertThat(registry.resolve("JDBC:H2:MEM:test"), sameInstance(GenericDialect.INSTANCE));
    }

    public void testResolveReturnsJdbcDialectType() {
        assertThat(registry.resolve("jdbc:postgresql://h/db"), instanceOf(JdbcDialect.class));
    }

    public void testConstructorRejectsNonLowercasePrefix() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DialectRegistry(Map.of("jdbc:POSTGRESQL://", PostgresDialect.INSTANCE), GenericDialect.INSTANCE)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("lowercase"));
    }

    public void testConstructorRejectsNullDefault() {
        expectThrows(IllegalArgumentException.class, () -> new DialectRegistry(Map.of(), null));
    }

    public void testConstructorRejectsEmptyPrefix() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new DialectRegistry(Map.of("", PostgresDialect.INSTANCE), GenericDialect.INSTANCE)
        );
    }

    public void testCustomRegistryDispatchAndDefault() {
        // A hand-built registry with a different default proves the seam is general, not hard-wired to generic.
        DialectRegistry custom = new DialectRegistry(Map.of("jdbc:postgresql://", GenericDialect.INSTANCE), PostgresDialect.INSTANCE);
        assertThat(custom.resolve("jdbc:postgresql://h/db"), sameInstance(GenericDialect.INSTANCE));
        assertThat(custom.resolve("jdbc:h2:mem:x"), sameInstance(PostgresDialect.INSTANCE));
    }
}
