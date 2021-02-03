/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.loggerusage;

import org.apache.logging.log4j.message.ParameterizedMessage;

/**
 * This class is for testing that <code>ESLoggerUsageChecker</code> can find incorrect usages of LogMessages
 * which are subclasses of <code>ParametrizedMessage</code>
 * @see ESLoggerUsageTests
 */
class TestMessage extends ParameterizedMessage {
    TestMessage(String messagePattern, String xOpaqueId, Object... args) {
        super(messagePattern, args);
    }
}
