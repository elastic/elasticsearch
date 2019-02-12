/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.Statement;

final class ConnectionProxy extends DebuggingInvoker {

    ConnectionProxy(DebugLog log, Object target) {
        super(log, target, null);
    }

    @Override
    protected Object postProcess(Object result, Object proxy) {
        if (result instanceof Statement) {
            return Debug.proxy(result, new StatementProxy(log, result, proxy));
        }
        if (result instanceof DatabaseMetaData) {
            return Debug.proxy(new DatabaseMetadataProxy(log, result, proxy));
        }

        return result;
    }
}
