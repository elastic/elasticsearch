/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.sl4j.bridge;

import org.elasticsearch.logging.LogManager;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

public class ESLoggerFactory implements ILoggerFactory {
    @Override
    public Logger getLogger(String name) {
        org.elasticsearch.logging.Logger logger = LogManager.getLogger(name);
        return new ESLogger(logger);
    }
}
