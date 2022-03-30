/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.logging;

import org.elasticsearch.logging.spi.LoggingBootstrapSupport;

public class JsonLogsTestSetup {
    private static boolean initialized = false;

    public static void init() {
        if (initialized == false) {
            LoggingBootstrapSupport.provider().setNodeName("sample-name");
            initialized = true;
        }
    }
}
