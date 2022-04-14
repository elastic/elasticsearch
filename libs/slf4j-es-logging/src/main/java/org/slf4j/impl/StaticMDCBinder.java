/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.slf4j.impl;

import org.slf4j.spi.MDCAdapter;

public class StaticMDCBinder {
    public static final StaticMDCBinder SINGLETON = new StaticMDCBinder();

    private StaticMDCBinder() {}

    /**
     * Returns the {@link #SINGLETON} {@link StaticMDCBinder}.
     * Added to slf4j-api 1.7.14 via https://github.com/qos-ch/slf4j/commit/ea3cca72cd5a9329a06b788317a17e806ee8acd0
     *
     * @return the singleton instance
     */
    public static StaticMDCBinder getSingleton() {
        return SINGLETON;
    }

    public MDCAdapter getMDCA() {
        throw new UnsupportedOperationException("MDC is unsupported");
    }

    public String getMDCAdapterClassStr() {
        throw new UnsupportedOperationException("MDC is unsupported");
    }
}
