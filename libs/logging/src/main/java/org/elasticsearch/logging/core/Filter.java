/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.core;

public interface Filter {

    Result filter(LogEvent logEvent);

    Filter.Result filterMessage(org.elasticsearch.logging.Message message);

    enum Result {
        /**
         * The event will be processed without further filtering based on the log Level.
         */
        ACCEPT,
        /**
         * No decision could be made, further filtering should occur.
         */
        NEUTRAL,
        /**
         * The event should not be processed.
         */
        DENY;
    }
}
