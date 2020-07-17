/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.ResultSet;

final class DatabaseMetaDataProxy extends DebuggingInvoker {

    DatabaseMetaDataProxy(DebugLog log, Object result, Object parent) {
        super(log, result, parent);
    }

    @Override
    protected Object postProcess(Object result, Object proxy) {
        if (result instanceof ResultSet) {
            return Debug.proxy(new ResultSetProxy(log, result, null));
        }
        return result;
    }
}
