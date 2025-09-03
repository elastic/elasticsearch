/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

record TransportVersionId(int complete, int base, int patch) implements Comparable<TransportVersionId> {

    public static TransportVersionId fromInt(int complete) {
        int patch = complete % 1000;
        int base = complete - patch;

        return new TransportVersionId(complete, base, patch);
    }

    static TransportVersionId fromString(String s) {
        return fromInt(Integer.parseInt(s));
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
}
