/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class SignedByteSizeValueTests extends ESTestCase {
    public void testTypeCapability() {
        ByteSizeValue value = new SignedByteSizeValue(-2L, ByteSizeUnit.BYTES);
        assertEquals(value.getBytes(), -2L);
        assertEquals(value.getUnit(), ByteSizeUnit.BYTES);
        assertEquals(value.toString(), "-2b");

        assertEquals(new SignedByteSizeValue(-2L, ByteSizeUnit.MB).toString(), "-2mb");
        assertEquals(new SignedByteSizeValue(-2L, ByteSizeUnit.GB).toString(), "-2gb");
        assertEquals(new SignedByteSizeValue(-2L, ByteSizeUnit.TB).toString(), "-2tb");
        assertEquals(new SignedByteSizeValue(-2L, ByteSizeUnit.PB).toString(), "-2pb");
    }

    public void testAddition() {
        assertThat(SignedByteSizeValue.add(SignedByteSizeValue.ZERO, SignedByteSizeValue.ZERO), is(SignedByteSizeValue.ZERO));
        assertThat(SignedByteSizeValue.add(SignedByteSizeValue.ZERO, SignedByteSizeValue.ONE), is(SignedByteSizeValue.ONE));
        assertThat(SignedByteSizeValue.add(SignedByteSizeValue.ONE, SignedByteSizeValue.ONE), is(SignedByteSizeValue.ofBytes(2L)));
        assertThat(SignedByteSizeValue.add(SignedByteSizeValue.MINUS_ONE, SignedByteSizeValue.ONE), is(SignedByteSizeValue.ZERO));
        assertThat(SignedByteSizeValue.add(SignedByteSizeValue.ZERO, SignedByteSizeValue.MINUS_ONE), is(SignedByteSizeValue.MINUS_ONE));
        assertThat(
            SignedByteSizeValue.add(SignedByteSizeValue.MINUS_ONE, SignedByteSizeValue.MINUS_ONE),
            is(SignedByteSizeValue.ofBytes(-2L))
        );
        assertThat(
            SignedByteSizeValue.add(SignedByteSizeValue.ofBytes(100L), SignedByteSizeValue.ONE),
            is(SignedByteSizeValue.ofBytes(101L))
        );
        assertThat(
            SignedByteSizeValue.add(SignedByteSizeValue.ofBytes(100L), SignedByteSizeValue.ofBytes(2L)),
            is(SignedByteSizeValue.ofBytes(102L))
        );
    }

    public void testSubtraction() {
        assertThat(SignedByteSizeValue.subtract(SignedByteSizeValue.ZERO, SignedByteSizeValue.ZERO), is(SignedByteSizeValue.ZERO));
        assertThat(SignedByteSizeValue.subtract(SignedByteSizeValue.ONE, SignedByteSizeValue.ZERO), is(SignedByteSizeValue.ONE));
        assertThat(SignedByteSizeValue.subtract(SignedByteSizeValue.ONE, SignedByteSizeValue.ONE), is(SignedByteSizeValue.ZERO));
        assertThat(
            SignedByteSizeValue.subtract(SignedByteSizeValue.MINUS_ONE, SignedByteSizeValue.ONE),
            is(SignedByteSizeValue.ofBytes(-2L))
        );
        assertThat(SignedByteSizeValue.subtract(SignedByteSizeValue.ZERO, SignedByteSizeValue.MINUS_ONE), is(SignedByteSizeValue.ONE));
        assertThat(SignedByteSizeValue.subtract(SignedByteSizeValue.ZERO, SignedByteSizeValue.ONE), is(SignedByteSizeValue.MINUS_ONE));
        assertThat(
            SignedByteSizeValue.subtract(SignedByteSizeValue.MINUS_ONE, SignedByteSizeValue.MINUS_ONE),
            is(SignedByteSizeValue.ZERO)
        );
        assertThat(
            SignedByteSizeValue.subtract(SignedByteSizeValue.ofBytes(100L), SignedByteSizeValue.ONE),
            is(SignedByteSizeValue.ofBytes(99L))
        );
        assertThat(
            SignedByteSizeValue.subtract(SignedByteSizeValue.ofBytes(100L), SignedByteSizeValue.ofBytes(2L)),
            is(SignedByteSizeValue.ofBytes(98L))
        );
        assertThat(
            SignedByteSizeValue.subtract(SignedByteSizeValue.ofBytes(100L), SignedByteSizeValue.ofBytes(102L)),
            is(SignedByteSizeValue.ofBytes(-2L))
        );
    }

    public void testMinimum() {
        assertThat(SignedByteSizeValue.min(SignedByteSizeValue.MINUS_ONE, SignedByteSizeValue.ONE), is(SignedByteSizeValue.MINUS_ONE));
        assertThat(SignedByteSizeValue.min(SignedByteSizeValue.ONE, SignedByteSizeValue.MINUS_ONE), is(SignedByteSizeValue.MINUS_ONE));
        assertThat(
            SignedByteSizeValue.min(SignedByteSizeValue.MINUS_ONE, SignedByteSizeValue.MINUS_ONE),
            is(SignedByteSizeValue.MINUS_ONE)
        );
        assertThat(
            SignedByteSizeValue.min(SignedByteSizeValue.ofBytes(-2L), SignedByteSizeValue.MINUS_ONE),
            is(SignedByteSizeValue.ofBytes(-2L))
        );
    }
}
