/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Standard test case for testing wire serialization. If the class being tested
 * extends {@link Writeable} then prefer extending {@link AbstractWireSerializingTestCase}.
 */
public abstract class AbstractWireTestCase<T> extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    /**
     * Returns an instance which is mutated slightly so it should not be equal
     * to the given instance.
     */
    protected abstract T mutateInstance(T instance) throws IOException;

    /**
     * Tests that the equals and hashcode methods are consistent and copied
     * versions of the instance are equal.
     */
    public final void testEqualsAndHashcode() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copyInstance, this::mutateInstance);
        }
    }

    /**
     * Calls {@link Object#equals} on equal objects on many threads and verifies
     * they all return true. Folks tend to assume this is true about
     * {@link Object#equals} and it <strong>generally</strong> is. But some
     * equals implementations violate this assumption and it's very surprising.
     * This tries to fail when that assumption is violated.
     */
    public final void testConcurrentEquals() throws IOException, InterruptedException, ExecutionException {
        T testInstance = createTestInstance();
        T copy = copyInstance(testInstance);

        /*
         * 500 rounds seems to consistently reproduce the issue on Nik's
         * laptop. Larger numbers are going to be slower but more likely
         * to reproduce the issue.
         */
        int rounds = scaledRandomIntBetween(300, 5000);
        concurrentTest(() -> {
            for (int r = 0; r < rounds; r++) {
                assertEquals(testInstance, copy);
            }
        });
    }

    /**
     * Call some test on many threads in parallel.
     */
    protected void concurrentTest(Runnable r) throws InterruptedException, ExecutionException {
        int threads = 5;
        int tasks = threads * 2;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int t = 0; t < tasks; t++) {
                results.add(exec.submit(r));
            }
            for (Future<?> f : results) {
                f.get();
            }
        } finally {
            exec.shutdown();
        }
    }

    /**
     * Calls {@link Object#hashCode} on the same object on many threads and
     * verifies they return the same result. Folks tend to assume this is true
     * about {@link Object#hashCode} and it <strong>generally</strong> is. But
     * some hashCode implementations violate this assumption and it's very
     * surprising. This tries to fail when that assumption is violated.
     */
    public final void testConcurrentHashCode() throws InterruptedException, ExecutionException {
        T testInstance = createTestInstance();
        int firstHashCode = testInstance.hashCode();

        /*
         * 500 rounds seems to consistently reproduce the issue on Nik's
         * laptop. Larger numbers are going to be slower but more likely
         * to reproduce the issue.
         */
        int rounds = scaledRandomIntBetween(300, 5000);
        concurrentTest(() -> {
            for (int r = 0; r < rounds; r++) {
                assertEquals(firstHashCode, testInstance.hashCode());
            }
        });
    }

    public void testToString() throws Exception {
        final String toString = createTestInstance().toString();
        assertNotNull(toString);
        assertThat(toString, not(emptyString()));
    }

    /**
     * Test serialization and deserialization of the test instance.
     */
    public final void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    /**
     * Test serializing the same object on many threads always
     * deserializes to equal instances. Folks tend to assume this is true
     * about serialization and it <strong>generally</strong> is. But
     * some implementations violate this assumption and it's very
     * surprising. This tries to fail when that assumption is violated.
     * <p>
     * Async search can serialize responses concurrently with other
     * operations like {@link ToXContent#toXContent}. This doesn't
     * check that exactly, but it's close.
     */
    public final void testConcurrentSerialization() throws InterruptedException, ExecutionException {
        T testInstance = createTestInstance();

        /*
         * 500 rounds seems to consistently reproduce the issue on Nik's
         * laptop. Larger numbers are going to be slower but more likely
         * to reproduce the issue.
         */
        int rounds = scaledRandomIntBetween(300, 2000);
        concurrentTest(() -> {
            try {
                for (int r = 0; r < rounds; r++) {
                    assertSerialization(testInstance);
                }
            } catch (IOException e) {
                throw new AssertionError("error serializing", e);
            }
        });
    }

    /**
     * Serialize the given instance and asserts that both are equal.
     */
    protected final void assertSerialization(T testInstance) throws IOException {
        assertSerialization(testInstance, Version.CURRENT);
    }

    /**
     * Assert that instances copied at a particular version are equal. The version is useful
     * for sanity checking the backwards compatibility of the wire. It isn't a substitute for
     * real backwards compatibility tests but it is *so* much faster.
     */
    protected final void assertSerialization(T testInstance, Version version) throws IOException {
        T deserializedInstance = copyInstance(testInstance, version);
        assertEqualInstances(testInstance, deserializedInstance);
    }

    /**
     * Assert that two instances are equal. This is intentionally not final so we can override
     * how equality is checked.
     */
    protected void assertEqualInstances(T expectedInstance, T newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.hashCode(), equalTo(expectedInstance.hashCode()));
    }

    protected final T copyInstance(T instance) throws IOException {
        return copyInstance(instance, Version.CURRENT);
    }

    /**
     * Copy the instance as by reading and writing using the code specific to the provided version.
     * The version is useful for sanity checking the backwards compatibility of the wire. It isn't
     * a substitute for real backwards compatibility tests but it is *so* much faster.
     */
    protected abstract T copyInstance(T instance, Version version) throws IOException;

    /**
     * Get the {@link NamedWriteableRegistry} to use when de-serializing the object.
     *
     * Override this method if you need to register {@link NamedWriteable}s for the test object to de-serialize.
     *
     * By default this will return a {@link NamedWriteableRegistry} with no registered {@link NamedWriteable}s
     */
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Collections.emptyList());
    }
}
