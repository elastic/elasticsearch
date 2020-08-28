/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.Statement;

class ResultSetProxy extends DebuggingInvoker {

    ResultSetProxy(DebugLog log, Object target, Object parent) {
        super(log, target, parent);
    }

    @Override
    protected Object postProcess(Object result, Object proxy) {
        if (result instanceof ResultSetMetaData) {
            return Debug.proxy(new ResultSetMetaDataProxy(log, result));
        }
        if (result instanceof Statement) {
            return parent;
        }
        return result;
    }
}
