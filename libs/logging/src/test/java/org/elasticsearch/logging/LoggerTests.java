/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging;

import org.elasticsearch.test.ESTestCase;

//TODO PG maybe we should have a separate testing module that tests API with our configs, layouts etc?
public class LoggerTests extends ESTestCase {

    public void testLogCreation() {
        Logger logger = LogManager.getLogger(LoggerTests.class);
        assertNotNull(logger);
    }
}
