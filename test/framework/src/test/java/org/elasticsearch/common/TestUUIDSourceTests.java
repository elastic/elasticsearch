/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.Executors;

public class TestUUIDSourceTests extends ESTestCase {

    public void testSeededSourceResetsBetweenInvocationsOnReusedWorker() throws Exception {
        final long seed = randomLong();
        final var worker = Executors.newSingleThreadExecutor();
        try {
            final CheckedSupplier<List<String>, Exception> generateOnWorker = () -> worker.submit(TestUUIDSourceTests::generateIds).get();
            final var expected = runRule(seed, uuidSource, generateOnWorker);
            assertNotEquals(expected, runRule(seed ^ 1L, uuidSource, generateOnWorker));
            assertEquals(expected, runRule(seed, uuidSource, generateOnWorker));
        } finally {
            assertTrue(terminate(worker));
        }
    }

    public void testNestedRulesRestoreSuiteSequence() throws Exception {
        assertSuiteSequenceRestored(false);
    }

    public void testNestedRulesRestoreSuiteSequenceOnFailure() throws Exception {
        assertSuiteSequenceRestored(true);
    }

    private void assertSuiteSequenceRestored(boolean failMethod) throws Exception {
        final long seed = randomLong();
        final var expected = runRule(seed, SUITE_UUID_SOURCE, () -> {
            final var ids = generateIds();
            ids.addAll(generateIds());
            return ids;
        });
        final var actual = runRule(seed, SUITE_UUID_SOURCE, () -> {
            final var ids = generateIds();
            if (failMethod) {
                final var failure = new IOException("test failure");
                assertSame(failure, expectThrows(IOException.class, () -> runRule(seed ^ 1L, uuidSource, () -> {
                    generateIds();
                    throw failure;
                })));
            } else {
                runRule(seed ^ 1L, uuidSource, TestUUIDSourceTests::generateIds);
            }
            ids.addAll(generateIds());
            return ids;
        });
        assertEquals(expected, actual);
    }

    public void testUUIDFormatAndRoutingHash() {
        final var decoder = Base64.getUrlDecoder();
        assertEquals(15, decoder.decode(UUIDs.base64UUID()).length);
        assertEquals(15, decoder.decode(UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.empty())).length);
        final int hash = randomInt();
        final String id = UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.of(hash));
        assertEquals(26, id.length());
        final var decoded = decoder.decode(id);
        assertEquals(19, decoded.length);
        assertEquals(hash, ByteUtils.readIntLE(decoded, decoded.length - 9));
    }

    private static List<String> runRule(long seed, TestRule rule, CheckedSupplier<List<String>, Exception> body) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed, () -> {
            final var ids = new ArrayList<String>();
            final var statement = rule.apply(new Statement() {
                @Override
                public void evaluate() throws Exception {
                    ids.addAll(body.get());
                }
            }, Description.EMPTY);
            try {
                statement.evaluate();
            } catch (Exception e) {
                throw e;
            } catch (Throwable t) {
                throw new AssertionError("UUID lifecycle rule failed", t);
            }
            return ids;
        });
    }

    private static List<String> generateIds() {
        final var ids = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            ids.add(UUIDs.base64UUID());
            ids.add(UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.empty()));
            ids.add(UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.of(i)));
        }
        return ids;
    }
}
