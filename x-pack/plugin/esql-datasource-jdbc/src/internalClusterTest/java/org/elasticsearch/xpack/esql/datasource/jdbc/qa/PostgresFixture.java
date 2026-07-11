/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc.qa;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.Properties;

/**
 * A {@link JdbcDatabaseFixture} backed by a real {@code postgres:16.4} container started with testcontainers.
 * Unlike in-process H2 the whole database lives in a container, so
 * {@link #startDatabase()} boots the container and {@link #esqlJdbcUrl()} is only well-defined afterwards.
 * <p>
 * <b>One container, many tables (shared by reference-count).</b> The shared correctness matrix declares three fixture
 * tables ({@code types_matrix}, {@code employees}, {@code edge_cases}) and {@link AbstractJdbcDatabaseIT} calls
 * {@link AbstractJdbcDatabaseIT#createFixture()} once per table. For Postgres it would be wasteful (and, on the
 * {@code vfs} storage driver used on the CI/dev VM, slow) to boot three containers, and — more to the point — a real
 * Postgres database is exactly the "many tables behind one JDBC URL" topology the {@code table}-aware schema
 * resolution targets. So every {@code PostgresFixture} instance shares a single static container: the first {@link
 * #startDatabase()} boots it, later ones just attach, and it is stopped only when the last fixture is {@link #stop()
 * stopped}. All three tables are therefore created (with distinct names) in the one database behind the one
 * {@link #esqlJdbcUrl() URL}; distinguishing their schemas is precisely what the {@code table}-aware
 * (non-cacheable) JDBC schema resolution does.
 * <p>
 * <b>Reliable teardown (put-before-start).</b> The container reference is stored in the static field
 * <em>before</em> {@link PostgreSQLContainer#start()} is called, so a {@code start()} that throws part-way still
 * leaves a reference the fixture can {@link PostgreSQLContainer#stop() stop}; a failed boot resets the field so a
 * later instance can retry. Reference counting is guarded by {@link #LOCK} and each instance decrements at most once
 * (tracked by {@link #counted}), so the base class's "stop every created fixture, even a half-started one" cleanup
 * (which may call {@link #stop()} on an instance whose {@link #startDatabase()} never ran) can never drive the count
 * negative or double-stop the container.
 */
public final class PostgresFixture extends JdbcDatabaseFixture {

    private static final DockerImageName IMAGE = DockerImageName.parse("postgres:16.4");

    /**
     * The container's superuser password. Set deliberately to a distinctive, unusual value (rather than inheriting the
     * testcontainers default {@code "test"}, which is far too common a substring to be a meaningful probe) so that the
     * connection-drop test ({@code PostgresJdbcIT#testConnectionDropSurfacesSanitizedError}) can use it as a
     * credential-leak SENTINEL: it is the REAL password (so authentication succeeds even when the value also rides the
     * JDBC URL query string, which pgjdbc prioritizes over connection {@code Properties}), yet a value this unusual
     * would never appear in a surfaced/sanitized error unless the sanitizer failed to scrub a credential.
     */
    public static final String CONTAINER_PASSWORD = "S3NT1NEL_pw_must_not_leak_9f3a2b";

    /** Guards {@link #container} and {@link #refCount}: instances are created/stopped across the test lifecycle. */
    private static final Object LOCK = new Object();

    /** The single shared container, or {@code null} when no fixture currently holds it. */
    private static PostgreSQLContainer<?> container;

    /** Number of started (not-yet-stopped) fixtures attached to {@link #container}; the container dies at 0. */
    private static int refCount;

    /** Whether this instance has incremented {@link #refCount} and so must decrement exactly once in {@link #stopDatabase()}. */
    private boolean counted;

    @Override
    protected void startDatabase() throws Exception {
        synchronized (LOCK) {
            if (container == null) {
                PostgreSQLContainer<?> pending = new PostgreSQLContainer<>(IMAGE).withPassword(CONTAINER_PASSWORD);
                // put-before-start: publish the reference before start() so a partially-started container is still
                // reachable for teardown.
                container = pending;
                try {
                    pending.start();
                } catch (Exception e) {
                    // Boot failed: undo the publish so the next fixture can retry, and best-effort release anything
                    // start() may have allocated before throwing.
                    container = null;
                    try {
                        pending.stop();
                    } catch (Exception suppressed) {
                        e.addSuppressed(suppressed);
                    }
                    throw e;
                }
            }
            refCount++;
            counted = true;
        }
    }

    @Override
    protected void stopDatabase() {
        synchronized (LOCK) {
            if (counted == false) {
                // startDatabase() never completed for this instance (e.g. the base class is cleaning up a fixture
                // whose start() failed before booting): nothing to release, and we must not touch the shared count.
                return;
            }
            counted = false;
            refCount--;
            if (refCount == 0 && container != null) {
                try {
                    container.stop();
                } finally {
                    container = null;
                }
            }
        }
    }

    @Override
    public String esqlJdbcUrl() {
        synchronized (LOCK) {
            if (container == null) {
                throw new IllegalStateException("Postgres container not started; call start() first");
            }
            // main's ExternalSourceResolver keys datasets on a StoragePath whose glob detection inspects the path
            // component, and '?' / '*' there are glob metacharacters. testcontainers appends a "?loggerLevel=OFF"
            // query string to getJdbcUrl(), so the raw URL would be misclassified as a glob pattern and rejected.
            // The dataset resource must therefore be a CLEAN jdbc URL (scheme://host:port/db, no query string); any
            // driver tuning that used to ride the query string is carried as WITH "connection_properties" instead.
            return stripQueryString(container.getJdbcUrl());
        }
    }

    @Override
    protected String driverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    protected Properties connectionProperties() {
        Properties props = new Properties();
        synchronized (LOCK) {
            if (container != null) {
                props.setProperty("user", container.getUsername());
                props.setProperty("password", container.getPassword());
            }
        }
        return props;
    }

    /**
     * Freezes the shared container's processes (Docker {@code pause}) so an already-forwarded TCP connection can be
     * opened by the kernel but the Postgres backend never answers — the deterministic, non-flaky way to simulate a
     * mid-flight database drop for the connection-drop test. Must be paired with {@link #unpauseContainer()} in a
     * {@code finally}; pausing rather than {@link PostgreSQLContainer#stop() stopping} keeps the shared container alive
     * for the rest of the SUITE. A short {@code socketTimeout} on the failing query's URL bounds how long the frozen
     * read blocks. Uses the Docker client directly (the testcontainers container object exposes no pause helper).
     */
    public void pauseContainer() {
        synchronized (LOCK) {
            requireContainer();
            org.testcontainers.DockerClientFactory.instance().client().pauseContainerCmd(container.getContainerId()).exec();
        }
    }

    /** Resumes a container frozen by {@link #pauseContainer()} so the shared container is healthy for later tests. */
    public void unpauseContainer() {
        synchronized (LOCK) {
            requireContainer();
            org.testcontainers.DockerClientFactory.instance().client().unpauseContainerCmd(container.getContainerId()).exec();
        }
    }

    /** True iff the shared container is present and reports running — used by the connection-drop test's leak check. */
    public boolean isContainerRunning() {
        synchronized (LOCK) {
            return container != null && container.isRunning();
        }
    }

    private static void requireContainer() {
        if (container == null) {
            throw new IllegalStateException("Postgres container not started; call start() first");
        }
    }

    @Override
    protected Map<String, String> datasetConfigOverrides() {
        synchronized (LOCK) {
            if (container == null) {
                return Map.of();
            }
            // The in-cluster connector authenticates with the same container credentials; carried as WITH options so
            // JdbcConnectorFactory can open the connection at resolve/query time.
            return Map.of("user", container.getUsername(), "password", container.getPassword());
        }
    }
}
