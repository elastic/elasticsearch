/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TDigestStateTests extends ESTestCase {

    public void testMoreThan4BValues() {
        // Regression test for #19528
        // See https://github.com/tdunning/t-digest/pull/70/files#diff-4487072cee29b939694825647928f742R439
        TDigestState digest = TDigestState.create(100);
        for (int i = 0; i < 1000; ++i) {
            digest.add(randomDouble());
        }
        final int count = 1 << 29;
        for (int i = 0; i < 10; ++i) {
            digest.add(randomDouble(), count);
        }
        assertEquals(1000 + 10L * (1 << 29), digest.size());
        assertTrue(digest.size() > 2L * Integer.MAX_VALUE);
        final double[] quantiles = new double[] { 0, 0.1, 0.5, 0.9, 1, randomDouble() };
        Arrays.sort(quantiles);
        double prev = Double.NEGATIVE_INFINITY;
        for (double q : quantiles) {
            final double v = digest.quantile(q);
            logger.trace("q=" + q + ", v=" + v);
            assertThat(v, Matchers.either(Matchers.closeTo(prev, 0.0000001D)).or(Matchers.greaterThan(prev)));
            assertTrue("Unexpectedly low value: " + v, v >= 0.0);
            assertTrue("Unexpectedly high value: " + v, v <= 1.0);
            prev = v;
        }
    }

    public void testEqualsHashCode() {
        final TDigestState empty1 = new EmptyTDigestState();
        final TDigestState empty2 = new EmptyTDigestState();
        final TDigestState a = TDigestState.create(200);
        final TDigestState b = TDigestState.create(100);
        final TDigestState c = TDigestState.create(100);

        assertEquals(empty1, empty2);
        assertEquals(empty1.hashCode(), empty2.hashCode());

        assertNotEquals(a, b);
        assertNotEquals(a.hashCode(), b.hashCode());

        assertNotEquals(a, c);
        assertNotEquals(a.hashCode(), c.hashCode());

        assertEquals(b, c);
        assertEquals(b.hashCode(), c.hashCode());

        for (int i = 0; i < 100; i++) {
            double value = randomDouble();
            a.add(value);
            b.add(value);
            c.add(value);
        }

        assertNotEquals(a, b);
        assertNotEquals(a.hashCode(), b.hashCode());

        assertNotEquals(a, c);
        assertNotEquals(a.hashCode(), c.hashCode());

        assertEquals(b, c);
        assertEquals(b.hashCode(), c.hashCode());

        b.add(randomDouble());
        c.add(randomDouble());

        assertNotEquals(b, c);
        assertNotEquals(b.hashCode(), c.hashCode());
    }

    public void testHash() {
        final HashMap<String, TDigestState> map = new HashMap<>();
        final Set<TDigestState> set = new HashSet<>();
        final TDigestState empty1 = new EmptyTDigestState();
        final TDigestState empty2 = new EmptyTDigestState();
        final TDigestState a = TDigestState.create(200);
        final TDigestState b = TDigestState.create(100);
        final TDigestState c = TDigestState.create(100);

        a.add(randomDouble());
        b.add(randomDouble());
        c.add(randomDouble());
        expectThrows(UnsupportedOperationException.class, () -> empty1.add(randomDouble()));
        expectThrows(UnsupportedOperationException.class, () -> empty2.add(a));

        map.put("empty1", empty1);
        map.put("empty2", empty2);
        map.put("a", a);
        map.put("b", b);
        map.put("c", c);
        set.add(empty1);
        set.add(empty2);
        set.add(a);
        set.add(b);
        set.add(c);

        assertEquals(5, map.size());
        assertEquals(4, set.size());

        assertEquals(empty1, map.get("empty1"));
        assertEquals(empty2, map.get("empty2"));
        assertEquals(a, map.get("a"));
        assertEquals(b, map.get("b"));
        assertEquals(c, map.get("c"));

        assertTrue(set.stream().anyMatch(digest -> digest.equals(a)));
        assertTrue(set.stream().anyMatch(digest -> digest.equals(b)));
        assertTrue(set.stream().anyMatch(digest -> digest.equals(c)));
        assertTrue(set.stream().anyMatch(digest -> digest.equals(empty1)));
        assertTrue(set.stream().anyMatch(digest -> digest.equals(empty2)));
    }

    public void testFactoryMethods() {
        TDigestState fast = TDigestState.create(100);
        TDigestState anotherFast = TDigestState.create(100);
        TDigestState accurate = TDigestState.createOptimizedForAccuracy(100);
        TDigestState anotherAccurate = TDigestState.createUsingParamsFrom(accurate);

        for (int i = 0; i < 100; i++) {
            fast.add(i);
            anotherFast.add(i);
            accurate.add(i);
            anotherAccurate.add(i);
        }

        for (double p : new double[] { 0.1, 1, 10, 25, 50, 75, 90, 99, 99.9 }) {
            double q = p / 100;
            assertEquals(fast.quantile(q), accurate.quantile(q), 0.5);
            assertEquals(fast.quantile(q), anotherFast.quantile(q), 1e-5);
            assertEquals(accurate.quantile(q), anotherAccurate.quantile(q), 1e-5);
        }

        assertEquals(fast, anotherFast);
        assertEquals(accurate, anotherAccurate);
    }

    public void testOverrideDefaultType() {
        TDigestState fast = TDigestState.create(100);
        TDigestState accurate = TDigestState.createOptimizedForAccuracy(100);

        TDigestExecutionHint.setDefaultValue(TDigestExecutionHint.HIGH_ACCURACY.toString());
        TDigestState anotherAccurate = TDigestState.create(100, TDigestExecutionHint.DEFAULT);

        TDigestExecutionHint.setDefaultValue(TDigestExecutionHint.DEFAULT.toString());
        TDigestState anotherFast = TDigestState.create(100, TDigestExecutionHint.DEFAULT);

        for (int i = 0; i < 100; i++) {
            fast.add(i);
            anotherFast.add(i);
            accurate.add(i);
            anotherAccurate.add(i);
        }

        assertEquals(fast, anotherFast);
        assertEquals(accurate, anotherAccurate);
        assertNotEquals(fast, accurate);
        assertNotEquals(anotherFast, anotherAccurate);
    }

    private static TDigestState writeToAndReadFrom(TDigestState state, TransportVersion version) throws IOException {
        BytesRef serializedAggs = serialize(state, version);
        try (
            StreamInput in = new NamedWriteableAwareStreamInput(
                StreamInput.wrap(serializedAggs.bytes),
                new NamedWriteableRegistry(Collections.emptyList())
            )
        ) {
            in.setTransportVersion(version);
            return TDigestState.read(in);

        }
    }

    private static BytesRef serialize(TDigestState state, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            TDigestState.write(state, out);
            return out.bytes().toBytesRef();
        }
    }

    public void testSerialization() throws IOException {
        // Past default was the accuracy-optimized version.
        TDigestState state = TDigestState.create(100);
        TDigestState backwardsCompatible = TDigestState.createOptimizedForAccuracy(100);
        for (int i = 0; i < 1000; i++) {
            state.add(i);
            backwardsCompatible.add(i);
        }

        TDigestState serialized = writeToAndReadFrom(state, TransportVersion.V_8_500_014);
        assertEquals(serialized, state);

        TDigestState serializedBackwardsCompatible = writeToAndReadFrom(state, TransportVersion.V_8_500_000);
        assertEquals(serializedBackwardsCompatible, backwardsCompatible);
    }
}
