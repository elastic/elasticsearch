/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.test.ESTestCase.assertEquals;

/**
 * Enum serialization via {@link StreamOutput#writeEnum} and {@link StreamInput#readEnum(Class)} uses the enum value's ordinal on the wire.
 * Reordering the values in an enum, or adding a new value, will change the ordinals and is therefore a wire protocol change, but it's easy
 * to miss this fact in the context of a larger commit. To protect against this trap, any enums that we send over the wire should have a
 * test that uses {@link #assertEnumSerialization} to assert a fixed mapping between ordinals and values. That way, a change to the ordinals
 * will require a test change, and thus some thought about BwC.
 */
public class EnumSerializationTestUtils {
    private EnumSerializationTestUtils() {/* no instances */}

    /**
     * Assert that the enum constants of the given class are exactly the ones passed in explicitly as arguments, which fixes its wire
     * protocol when using {@link StreamOutput#writeEnum} and {@link StreamInput#readEnum(Class)}.
     *
     * @param value0 The enum constant with ordinal {@code 0}, passed as a separate argument to avoid prevent callers just lazily using
     *               {@code EnumClass.values()} to pass the values of the enum, which would negate the point of this test.
     * @param values The remaining enum constants, in ordinal order.
     */
    @SafeVarargs
    public static <E extends Enum<E>> void assertEnumSerialization(Class<E> clazz, E value0, E... values) {
        final var enumConstants = clazz.getEnumConstants();
        assertEquals(clazz.getCanonicalName(), enumConstants.length, values.length + 1);
        for (var ordinal = 0; ordinal < values.length + 1; ordinal++) {
            final var enumValue = ordinal == 0 ? value0 : values[ordinal - 1];
            final var description = clazz.getCanonicalName() + "[" + ordinal + "]";
            assertEquals(description, enumConstants[ordinal], enumValue);
            try (var out = new BytesStreamOutput(1)) {
                out.writeEnum(enumValue);
                try (var in = out.bytes().streamInput()) {
                    assertEquals(description, ordinal, in.readVInt());
                }
            } catch (IOException e) {
                ESTestCase.fail(e);
            }
            try (var out = new BytesStreamOutput(1)) {
                out.writeVInt(ordinal);
                try (var in = out.bytes().streamInput()) {
                    assertEquals(description, enumValue, in.readEnum(clazz));
                }
            } catch (IOException e) {
                ESTestCase.fail(e);
            }
        }
    }
}
