/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/** A count a test reads back after a single-threaded run, held by the directories and values that watch it. */
final class Counter {

    private long count;

    void increment() {
        count++;
    }

    void add(long amount) {
        count += amount;
    }

    long get() {
        return count;
    }
}
