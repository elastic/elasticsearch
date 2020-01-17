/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.type.DataTypeConverter.Converter;

import java.util.Collection;

import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;

public class DefaultDataTypeRegistry implements DataTypeRegistry {

    public static final DataTypeRegistry INSTANCE = new DefaultDataTypeRegistry();

    private DefaultDataTypeRegistry() {}

    @Override
    public Collection<DataType> dataTypes() {
        return DataTypes.TYPES;
    }

    @Override
    public DataType fromEs(String typeName) {
        return DataTypes.fromEs(typeName);
    }

    @Override
    public DataType fromJava(Object value) {
        return DataTypes.fromJava(value);
    }

    @Override
    public boolean isUnsupported(DataType type) {
        return type == UNSUPPORTED;
    }

    @Override
    public boolean canConvert(DataType from, DataType to) {
        return DataTypeConverter.canConvert(from, to);
    }

    @Override
    public Object convert(Object value, DataType type) {
        DataType detectedType = DataTypes.fromJava(value);

        if (detectedType == type || value == null) {
            return value;
        }

        Converter converter = DataTypeConverter.converterFor(detectedType, type);

        if (converter == null) {
            throw new QlIllegalArgumentException("cannot convert from [{}] to [{}]", value, type.typeName());
        }
        return DataTypeConverter.convert(value, type);
    }

    @Override
    public DataType commonType(DataType left, DataType right) {
        return DataTypeConverter.commonType(left, right);
    }
}