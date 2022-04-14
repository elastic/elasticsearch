/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.logging.es;

import org.apache.logging.log4j.spi.Provider;

public class ESLoggingProvider extends Provider {
    public ESLoggingProvider() {
        super(15, "2.6.0", ESLoggerContextFactory.class, MDCContextMap.class);
    }
}
