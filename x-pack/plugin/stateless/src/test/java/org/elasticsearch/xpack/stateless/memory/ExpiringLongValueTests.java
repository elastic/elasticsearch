/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

public class ExpiringLongValueTests extends ESTestCase {

    public void testKeepsTheLargerValueUntilItExpires() {
        final ExpiringLongValue expiring = new ExpiringLongValue();
        final long now = randomLongBetween(0L, 1_000_000L);
        final long validityNanos = randomLongBetween(2L, 1_000_000L);
        final long value = randomLongBetween(1L, 1_000_000L);
        final long smaller = randomLongBetween(0L, value - 1);
        final long larger = value + randomLongBetween(1L, 1_000L);

        assertThat(expiring.valueIfValid(now, 0L), equalTo(0L));
        assertThat(expiring.sampleIfValid(now), nullValue());

        expiring.update(value, now, validityNanos);
        assertThat(expiring.valueIfValid(now, 0L), equalTo(value));
        assertThat(expiring.sampleIfValid(now).value(), equalTo(value));
        assertThat(expiring.sampleIfValid(now).validUntil(), equalTo(now + validityNanos));

        expiring.update(smaller, now, validityNanos);
        assertThat(expiring.valueIfValid(now, 0L), equalTo(value));
        assertThat(expiring.sampleIfValid(now).validUntil(), equalTo(now + validityNanos));

        expiring.update(larger, now, validityNanos);
        assertThat(expiring.valueIfValid(now, 0L), equalTo(larger));
        assertThat(expiring.sampleIfValid(now).validUntil(), equalTo(now + validityNanos));

        final long deadline = now + validityNanos;
        assertThat(expiring.valueIfValid(deadline, 0L), equalTo(larger));

        final long afterExpiry = deadline + 1;
        assertThat(expiring.sampleIfValid(afterExpiry), nullValue());
        assertThat(expiring.valueIfValid(afterExpiry, 0L), equalTo(0L));

        expiring.update(smaller, afterExpiry, validityNanos);
        assertThat(expiring.valueIfValid(afterExpiry, 0L), equalTo(smaller));
        assertThat(expiring.sampleIfValid(afterExpiry).validUntil(), equalTo(afterExpiry + validityNanos));
    }

    public void testRepublishingTheSameValueExtendsExpiry() {
        final ExpiringLongValue expiring = new ExpiringLongValue();
        final long now = randomLongBetween(0L, 1_000_000L);
        final long validityNanos = randomLongBetween(2L, 1_000_000L);
        final long value = randomLongBetween(0L, 1_000_000L);

        expiring.update(value, now, validityNanos);
        final long firstDeadline = expiring.sampleIfValid(now).validUntil();

        final long later = now + randomLongBetween(1L, validityNanos);
        expiring.update(value, later, validityNanos);
        assertThat(expiring.sampleIfValid(later).value(), equalTo(value));
        assertThat(expiring.sampleIfValid(later).validUntil(), equalTo(later + validityNanos));
        assertThat(expiring.sampleIfValid(later).validUntil(), greaterThan(firstDeadline));

        expiring.update(value, now, validityNanos);
        assertThat(expiring.sampleIfValid(later).validUntil(), equalTo(later + validityNanos));
    }

    public void testClearDropsTheValue() {
        final ExpiringLongValue expiring = new ExpiringLongValue();
        final long now = randomLongBetween(0L, 1_000_000L);
        expiring.update(randomLongBetween(1L, 1_000_000L), now, randomLongBetween(1L, 1_000_000L));
        expiring.clear();
        assertThat(expiring.valueIfValid(now, 0L), equalTo(0L));
        assertThat(expiring.sampleIfValid(now), nullValue());
    }

    public void testValueIfValidReturnsTheCallerFallbackWhenMissingOrExpired() {
        final ExpiringLongValue expiring = new ExpiringLongValue();
        final long now = randomLongBetween(0L, 1_000_000L);
        final long fallback = randomLongBetween(1L, 1_000L);
        assertThat(expiring.valueIfValid(now, fallback), equalTo(fallback));

        final long validityNanos = randomLongBetween(1L, 1_000L);
        expiring.update(randomLongBetween(0L, 1_000L), now, validityNanos);
        final long afterExpiry = now + validityNanos + 1;
        assertThat(expiring.valueIfValid(afterExpiry, fallback), equalTo(fallback));
        assertThat(expiring.sampleIfValid(afterExpiry), nullValue());
    }

    public void testZeroIsAValidStoredValue() {
        final ExpiringLongValue expiring = new ExpiringLongValue();
        final long now = randomLongBetween(0L, 1_000_000L);
        expiring.update(0L, now, randomLongBetween(1L, 1_000L));
        assertThat(expiring.valueIfValid(now, -1L), equalTo(0L));
        assertThat(expiring.sampleIfValid(now).value(), equalTo(0L));
    }

    public void testUpdateRejectsANegativeValue() {
        final ExpiringLongValue expiring = new ExpiringLongValue();
        final long now = randomLongBetween(0L, 1_000_000L);
        final long stored = randomLongBetween(0L, 1_000L);
        expiring.update(stored, now, randomLongBetween(1L, 1_000L));

        final long negative = -1L - randomLongBetween(0L, 1_000L);
        expectThrows(IllegalArgumentException.class, () -> expiring.update(negative, now, randomLongBetween(1L, 1_000L)));
        assertThat(expiring.valueIfValid(now, -1L), equalTo(stored));
    }
}
