/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

record TransportVersionId(int complete, int major, int server, int subsidiary, int patch) implements Comparable<TransportVersionId> {

    /**
     * The TV format:
     * <p>
     * MM_NNN_S_PP
     * <p>
     * M - The major version of Elasticsearch
     * NNN - The server version part
     * S - The subsidiary version part. It should always be 0 here, it is only used in subsidiary repositories.
     * PP - The patch version part
     */
    public enum Component {
        MAJOR(1_000_0_00, 2),
        SERVER(1_0_00, 3),
        SUBSIDIARY(1_00, 1),
        PATCH(1, 2);

        private final int value;
        private final int max;

        Component(int value, int numDigits) {
            this.value = value;
            this.max = (int) Math.pow(10, numDigits);
        }
    }

    public static TransportVersionId fromInt(int complete) {
        int patch = complete % Component.PATCH.max;
        int subsidiary = (complete / Component.SUBSIDIARY.value) % Component.SUBSIDIARY.max;
        int server = (complete / Component.SERVER.value) % Component.SERVER.max;
        int major = complete / Component.MAJOR.value;

        return new TransportVersionId(complete, major, server, subsidiary, patch);
    }

    static TransportVersionId fromString(String s) {
        return fromInt(Integer.parseInt(s));
    }

    public TransportVersionId bumpComponent(Component component) {
        int zeroesCleared = (complete / component.value) * component.value;
        int newId = zeroesCleared + component.value;
        if ((newId / component.value) % component.max == 0) {
            throw new IllegalStateException(
                "Insufficient" + component.name() + " version section in TransportVersion: " + complete + ", Cannot bump."
            );
        }
        return fromInt(newId);
    }

    @Override
    public int compareTo(TransportVersionId o) {
        // note: this is descending order so the arguments are reversed
        return Integer.compare(o.complete, complete);
    }

    @Override
    public String toString() {
        return Integer.toString(complete);
    }

    public int base() {
        return (complete / 1000) * 1000;
    }
}
