/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.util.Map;

public interface CompoundDataType extends DataType {

    @Override
    default JDBCType sqlType() {
        return JDBCType.STRUCT;
    }

    @Override
    default int precision() {
        return 0;
    }

    @Override
    default boolean isInteger() {
        return false;
    }

    @Override
    default boolean isRational() {
        return false;
    }

    @Override
    default boolean isPrimitive() {
        return false;
    }

    @Override
    default boolean hasDocValues() {
        return false;
    }

    Map<String, DataType> properties();

}
