/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Base64;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;

public class TestUUIDSourceTests extends ESTestCase {

    public void testWithUUIDSourceOverridesWithinScope() {
        final var stub = randomStub();
        TestUUIDSource.withUUIDSource(stub, () -> assertStubActive(stub));
        assertDefaultActive();
    }

    public void testWithUUIDSourceRestoresDefaultWhenBodyThrows() {
        final var stub = randomStub();
        final var exception = new IOException(randomAlphaOfLength(10));
        final var thrown = expectThrows(IOException.class, () -> TestUUIDSource.withUUIDSource(stub, () -> {
            assertStubActive(stub);
            throw exception;
        }));
        assertSame("withUUIDSource should propagate the exception thrown by the body", exception, thrown);
        assertDefaultActive();
    }

    public void testWithUUIDSourceRejectsSecondScope() {
        final var outer = randomStub();
        final var inner = randomStub();
        final var innerBodyRan = new AtomicBoolean();
        TestUUIDSource.withUUIDSource(outer, () -> {
            final var error = expectThrows(AssertionError.class, () -> TestUUIDSource.withUUIDSource(inner, () -> innerBodyRan.set(true)));
            assertThat("rejection should name the active source", error.getMessage(), containsString(outer.toString()));
            assertFalse("rejected scope should skip its body", innerBodyRan.get());
            assertStubActive(outer);
        });
        assertDefaultActive();
    }

    private static StubUUIDSource randomStub() {
        return new StubUUIDSource("uuid-" + randomAlphaOfLength(5), "k-ordered-" + randomAlphaOfLength(5));
    }

    private static void assertStubActive(StubUUIDSource stub) {
        final var hash = randomBoolean() ? OptionalInt.empty() : OptionalInt.of(randomInt());
        assertEquals("base64UUID should come from " + stub + " within the scope", stub.base64UUID(), UUIDs.base64UUID());
        assertEquals(
            "k-ordered UUID with " + hash + " should come from " + stub + " within the scope",
            stub.base64TimeBasedKOrderedUUIDWithHash(hash),
            UUIDs.base64TimeBasedKOrderedUUIDWithHash(hash)
        );
    }

    private static void assertDefaultActive() {
        assertEquals(
            "base64UUID should come from the time-based default after the scope",
            UUIDs.TIME_BASED_UUID_STRING_LENGTH,
            UUIDs.base64UUID().length()
        );
        final var hash = randomInt();
        final var decoded = Base64.getUrlDecoder().decode(UUIDs.base64TimeBasedKOrderedUUIDWithHash(OptionalInt.of(hash)));
        assertEquals(
            "k-ordered UUID should embed the routing hash after the scope",
            hash,
            ByteUtils.readIntLE(decoded, decoded.length - 9)
        );
    }

    private record StubUUIDSource(String uuid, String kOrderedPrefix) implements UUIDSource {

        @Override
        public String base64UUID() {
            return uuid;
        }

        @Override
        public String base64TimeBasedKOrderedUUIDWithHash(OptionalInt hash) {
            return kOrderedPrefix + hash;
        }
    }
}
