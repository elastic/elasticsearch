/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

public interface Counter {
    void increment();

    void incrementBy(long inc);

    Counter NOOP = new Counter() {
        @Override
        public void increment() {

        }

        @Override
        public void incrementBy(long inc) {

        }
    };
}
