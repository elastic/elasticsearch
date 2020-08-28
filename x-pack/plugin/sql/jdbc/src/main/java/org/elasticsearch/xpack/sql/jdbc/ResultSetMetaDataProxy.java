/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

final class ResultSetMetaDataProxy extends DebuggingInvoker {

    ResultSetMetaDataProxy(DebugLog log, Object target) {
        super(log, target, null);
    }
}
