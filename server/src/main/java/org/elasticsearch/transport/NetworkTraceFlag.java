/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import java.util.Locale;

public class NetworkTraceFlag {

    private NetworkTraceFlag() {
        // no instances;
    }

    public static final String PROPERTY_NAME = "es.insecure_network_trace_enabled";

    public static final boolean TRACE_ENABLED;

    static {
        final String propertyValue = System.getProperty(PROPERTY_NAME);
        if (propertyValue == null) {
            TRACE_ENABLED = false;
        } else if ("true".equals(propertyValue)) {
            TRACE_ENABLED = true;
        } else {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "system property [%s] may only be set to [true], but was [%s]", PROPERTY_NAME, propertyValue)
            );
        }
    }
}
