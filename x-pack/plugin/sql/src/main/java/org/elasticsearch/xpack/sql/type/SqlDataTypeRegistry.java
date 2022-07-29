/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeRegistry;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Collection;

public class SqlDataTypeRegistry implements DataTypeRegistry {

    public static final DataTypeRegistry INSTANCE = new SqlDataTypeRegistry();

    private SqlDataTypeRegistry() {}

    @Override
    public Collection<DataType> dataTypes() {
        return SqlDataTypes.types();
    }

    @Override
    public DataType fromEs(String typeName) {
        return SqlDataTypes.fromEs(typeName);
    }

    @Override
    public DataType fromJava(Object value) {
        return SqlDataTypes.fromJava(value);
    }

    @Override
    public boolean isUnsupported(DataType type) {
        return DataTypes.isUnsupported(type);
    }

    @Override
    public boolean canConvert(DataType from, DataType to) {
        return SqlDataTypeConverter.canConvert(from, to);
    }

    @Override
    public Object convert(Object value, DataType type) {
        return SqlDataTypeConverter.convert(value, type);
    }

    @Override
    public DataType commonType(DataType left, DataType right) {
        return SqlDataTypeConverter.commonType(left, right);
    }

}
