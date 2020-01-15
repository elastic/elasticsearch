/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeRegistry;

import java.util.Collection;

public class SqlDataTypeRegistry implements DataTypeRegistry {

    @Override
    public Collection<DataType> dataTypes() {
        return null;
    }

    @Override
    public DataType fromJava(Object value) {
        return null;
    }

    @Override
    public boolean isUnsupported(DataType type) {
        return false;
    }

    @Override
    public boolean canConvert(DataType from, DataType to) {
        return false;
    }

    @Override
    public Object convert(Object value, DataType type) {
        return null;
    }

    @Override
    public DataType commonType(DataType left, DataType right) {
        return null;
    }

}
