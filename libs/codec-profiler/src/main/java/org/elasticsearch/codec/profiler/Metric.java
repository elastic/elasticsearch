/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.codec.profiler;

import java.util.Comparator;
import java.util.Objects;

/**
 * A metric definition: a quantity that a codec counts or times during reads. A codec declares its metrics as constants
 * next to the code that records them, so no central list changes when a codec adds a metric. The definition holds the
 * identity, the unit and the description and, for a timer, the {@link TimingPolicy} and the optional CPU time
 * companion. A collector refuses two different definitions of one identity, and so does a merge, so a measurement
 * keeps the meaning it was recorded with.
 *
 * <p>{@link CounterMetric} and {@link TimerMetric} are separate types, so the compiler refuses a counter definition
 * where a timer definition is required, and the reverse.
 *
 * <p>The name shows the unit, so a reader of a profile knows the unit without a lookup: a byte count has {@code bytes}
 * in its name, a span of document ids has {@code docid}, and a time ends in {@code _time_nanos}.
 */
public sealed interface Metric permits CounterMetric, TimerMetric {

    /** The identity. Snapshots key, order and group measurements by it. */
    Id id();

    /** The unit. Every timer has {@link Unit#TIME_NANOS}. */
    Unit unit();

    /** What the value means, including when it is recorded and what it leaves out. */
    String description();

    /** What a metric counts. */
    enum Unit {
        /** Occurrences of an event. */
        COUNT,
        /** Bytes, whatever layer they belong to. */
        BYTES,
        /** Document ids spanned, which can include documents that hold no value. */
        DOC_IDS,
        /** Nanoseconds of a call: elapsed time, or thread CPU time for a CPU time companion. */
        TIME_NANOS
    }

    /**
     * The identity of a metric: the codec that owns it, the part of the codec it measures, and its own name. An
     * identity has one unit and one meaning, permanently. Consumers sum the values of equal identities without a check,
     * so a change of definition is a breaking change.
     *
     * <p>A group is the codec's own name, letters, digits and underscores starting with a letter, so a profile can show
     * it as the codec is called. A component and a name are each lower case words joined by underscores. None of them
     * holds a dot, so the dots of {@link #key()} always separate exactly these three parts.
     *
     * @param group     the identifier of the codec that owns the metric, so metrics of different owners never collide
     * @param component the part of the codec the metric measures, such as its blocks or its dictionary; it groups
     *                  related metrics in a profile
     * @param name      the metric's own name within its component
     */
    record Id(String group, String component, String name) implements Comparable<Id> {

        private static final Comparator<Id> ORDER = Comparator.comparing(Id::group).thenComparing(Id::component).thenComparing(Id::name);

        public Id {
            Objects.requireNonNull(group, "group");
            Objects.requireNonNull(component, "component");
            Objects.requireNonNull(name, "name");
            if (isIdentifier(group) == false) {
                throw new IllegalArgumentException(
                    "metric group [" + group + "] must be letters, digits and underscores starting with a letter"
                );
            }
            if (isWellFormed(component) == false) {
                throw new IllegalArgumentException("metric component [" + component + "] must be lower case words joined by underscores");
            }
            if (isWellFormed(name) == false) {
                throw new IllegalArgumentException("metric name [" + name + "] must be lower case words joined by underscores");
            }
        }

        /** The flat form {@code group.component.name}, which is how a metric crosses the wire. */
        public String key() {
            return group + "." + component + "." + name;
        }

        /**
         * The identity whose {@link #key()} is {@code key}. This is for keys that arrive from another node, which wrote
         * them from validated identities, so a key that is not one is refused.
         *
         * @throws IllegalArgumentException when {@code key} is not the key of a valid identity
         */
        public static Id parse(String key) {
            final int first = key.indexOf('.');
            final int second = first < 0 ? -1 : key.indexOf('.', first + 1);
            if (second < 0) {
                throw new IllegalArgumentException("[" + key + "] is not a metric key of the form group.component.name");
            }
            return new Id(key.substring(0, first), key.substring(first + 1, second), key.substring(second + 1));
        }

        /**
         * Orders by group, then component, then name. No part holds a dot, and every character a part can hold sorts
         * after one, so this is also the order of the keys.
         */
        @Override
        public int compareTo(Id other) {
            return ORDER.compare(this, other);
        }

        @Override
        public String toString() {
            return key();
        }

        private static boolean isWellFormed(String part) {
            if (part.isEmpty() || part.charAt(0) < 'a' || part.charAt(0) > 'z') {
                return false;
            }
            for (int i = 1; i < part.length(); i++) {
                final char c = part.charAt(i);
                final boolean allowed = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
                if (allowed == false) {
                    return false;
                }
            }
            return true;
        }

        private static boolean isIdentifier(String group) {
            if (group.isEmpty()) {
                return false;
            }
            final char first = group.charAt(0);
            if ((first >= 'a' && first <= 'z') == false && (first >= 'A' && first <= 'Z') == false) {
                return false;
            }
            for (int i = 0; i < group.length(); i++) {
                final char c = group.charAt(i);
                final boolean allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
                if (allowed == false) {
                    return false;
                }
            }
            return true;
        }
    }
}
