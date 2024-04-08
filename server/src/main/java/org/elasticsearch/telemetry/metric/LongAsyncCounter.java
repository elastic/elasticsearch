/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.metric;

/**
 * A monotonically increasing long metric based on a callback.
 */
public interface LongAsyncCounter extends Instrument, AutoCloseable {

    /**
     * Noop counter for use in tests.
     */
    LongAsyncCounter NOOP = new LongAsyncCounter() {
        @Override
        public void close() throws Exception {

        }

        @Override
        public String getName() {
            return "noop";
        }
    };
}
