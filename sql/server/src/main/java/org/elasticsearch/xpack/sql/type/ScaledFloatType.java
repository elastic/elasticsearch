/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;

public class ScaledFloatType extends AbstractDataType {

    public ScaledFloatType(boolean docValues) {
        super(JDBCType.FLOAT, docValues);
    }

    @Override
    public String esName() {
        return "scaled_float";
    }

    @Override
    public int precision() {
        // just like long
        return 19;
    }
}
