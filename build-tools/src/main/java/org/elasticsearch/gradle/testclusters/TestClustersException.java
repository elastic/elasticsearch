/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

class TestClustersException extends RuntimeException {
    TestClustersException(String message) {
        super(message);
    }

    TestClustersException(String message, Throwable cause) {
        super(message, cause);
    }

    TestClustersException(Throwable cause) {
        super(cause);
    }
}
