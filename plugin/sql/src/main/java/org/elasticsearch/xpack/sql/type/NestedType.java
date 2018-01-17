/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Map;

public class NestedType extends CompoundDataType {

    public NestedType(Map<String, DataType> properties) {
        super(JDBCType.STRUCT, false, properties);
    }

    @Override
    public String esName() {
        return "nested";
    }

    @Override
    public String toString() {
        return "N" + properties();
    }
}