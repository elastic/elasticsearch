/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Proves the fail-fast concurrency behaviour of {@link JdbcHikariPool}: with {@code max_per_url=10} and 15 in-flight
 * borrowers against one endpoint, exactly 10 obtain a connection and the surplus 5 fail fast (within
 * {@code connectionTimeout}) with the translated {@link IllegalStateException} rather than blocking indefinitely.
 * <p>
 * This is the invariant that keeps a saturated JDBC endpoint from parking {@code esql_worker} threads forever:
 * a short {@code connectionTimeout} converts contention into a bounded failure. Runs
 * against in-process H2 -- no Docker.
 */
public class JdbcConcurrencyLimitTests extends ESTestCase {

    private JdbcDriverRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = JdbcDriverRegistry.fromClassLoader(getClass().getClassLoader());
    }

    @Override
    public void tearDown() throws Exception {
        registry.close();
        super.tearDown();
    }

    public void testSurplusBorrowersFailFastNotBlock() throws Exception {
        final int maxPerUrl = 10;
        final int borrowers = 15;
        final long connectionTimeoutMs = 1000L;

        JdbcRuntimeConfig config = new JdbcRuntimeConfig();
        config.initialize(
            Settings.builder()
                .put(JdbcRuntimeConfig.POOL_MAX_PER_URL.getKey(), maxPerUrl)
                .put(JdbcRuntimeConfig.POOL_CONNECTION_TIMEOUT_MS.getKey(), connectionTimeoutMs)
                .build()
        );

        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        // Keep-alive so the in-mem DB survives for the whole test regardless of pool churn.
        Connection keepAlive = registry.connect(url, new Properties());

        JdbcHikariPool pool = new JdbcHikariPool(registry, config);
        ExecutorService exec = Executors.newFixedThreadPool(borrowers);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger failFast = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        List<Connection> held = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < borrowers; i++) {
                exec.submit(() -> {
                    Connection conn = null;
                    try {
                        conn = pool.getConnection(url, new Properties());
                        held.add(conn);
                        successes.incrementAndGet();
                        // Hold the connection so it stays checked out while the surplus borrowers contend + time out.
                        release.await();
                    } catch (IllegalStateException e) {
                        if (e.getMessage() != null && e.getMessage().contains("no JDBC connection available")) {
                            failFast.incrementAndGet();
                        } else {
                            other.incrementAndGet();
                        }
                    } catch (Exception e) {
                        other.incrementAndGet();
                    } finally {
                        if (conn != null) {
                            try {
                                conn.close();
                            } catch (Exception ignored) {
                                // returning to the pool; ignore
                            }
                        }
                    }
                });
            }

            // The 5 surplus borrowers fail within ~connectionTimeout. Wait (bounded) until they have, i.e. until we
            // have observed 5 fail-fast outcomes. The 10 holders block on `release` and are counted by then.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (failFast.get() < (borrowers - maxPerUrl) && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }

            assertEquals("exactly max_per_url borrowers should hold a connection", maxPerUrl, successes.get());
            assertEquals("surplus borrowers must fail fast, not block", borrowers - maxPerUrl, failFast.get());
            assertEquals("no unexpected failures", 0, other.get());
        } finally {
            release.countDown();
            exec.shutdown();
            assertTrue("worker tasks must finish promptly once released", exec.awaitTermination(30, TimeUnit.SECONDS));
            pool.close();
            keepAlive.close();
        }
    }
}
