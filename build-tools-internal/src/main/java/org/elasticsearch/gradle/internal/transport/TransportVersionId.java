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

    static TransportVersionId fromString(String s) {
        int complete = Integer.parseInt(s);
        int patch = complete % 100;
        int subsidiary = (complete / 100) % 10;
        int server = (complete / 1000) % 1000;
        int major = complete / 1000000;
        return new TransportVersionId(complete, major, server, subsidiary, patch);
    }

    @Override
    public int compareTo(TransportVersionId o) {
        return Integer.compare(complete, o.complete);
    }

    @Override
    public String toString() {
        return Integer.toString(complete);
    }

    public int base() {
        return (complete / 1000) * 1000;
    }
}
