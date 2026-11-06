/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.sketch;

/**
 * The grouping key of a {@link SketchFilterOperator}: the channels of the key columns, and how the key of a row becomes one
 * {@code long}. The sketch counts that {@code long}, so it must identify the key without collisions:
 * <ul>
 * <li>{@link Kind#LONG}: one {@code long} column, used as it is.</li>
 * <li>{@link Kind#INT}: one {@code int} column, widened to a {@code long} (different values stay different).</li>
 * <li>{@link Kind#INT_PAIR}: two {@code int} columns packed as {@code (first << 32) | (second & 0xFFFFFFFF)}. Each 32-bit value has its
 * own half, so different pairs stay different.</li>
 * </ul>
 * A key that cannot be packed this way (a string, a {@code long} next to another column) is not supported, because a hash of it could
 * collide, and then the proof could not tell whether it passed.
 */
public record KeyColumns(Kind kind, int channel, int secondChannel) {
    public enum Kind {
        LONG,
        INT,
        INT_PAIR
    }

    public static KeyColumns longKey(int channel) {
        return new KeyColumns(Kind.LONG, channel, -1);
    }

    public static KeyColumns intKey(int channel) {
        return new KeyColumns(Kind.INT, channel, -1);
    }

    public static KeyColumns intPairKey(int firstChannel, int secondChannel) {
        return new KeyColumns(Kind.INT_PAIR, firstChannel, secondChannel);
    }

    /** The packed key of a pair of {@code int} values. Different pairs give different results. */
    static long pack(int first, int second) {
        return ((long) first << 32) | (second & 0xFFFFFFFFL);
    }
}
