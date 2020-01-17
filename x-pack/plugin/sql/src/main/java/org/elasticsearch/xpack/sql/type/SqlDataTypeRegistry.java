/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypeRegistry;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.expression.literal.interval.Interval;

import java.time.OffsetTime;
import java.util.Collection;

import static org.elasticsearch.xpack.sql.type.SqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.TIME;

public class SqlDataTypeRegistry implements DataTypeRegistry {

    public static final DataTypeRegistry INSTANCE = new SqlDataTypeRegistry();

    private SqlDataTypeRegistry() {}

    @Override
    public Collection<DataType> dataTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataType fromEs(String typeName) {
        return SqlDataTypes.fromJava(typeName);
    }

    @Override
    public DataType fromJava(Object value) {
        DataType type = DataTypes.fromJava(value);

        if (type != null) {
            return type;
        }

        if (value instanceof OffsetTime) {
            return TIME;
        }

        if (value instanceof GeoShape) {
            return GEO_SHAPE;
        }

        if (value instanceof Interval) {
            return ((Interval<?>) value).dataType();
        }

        return null;
    }

    @Override
    public boolean isUnsupported(DataType type) {
        return DataTypes.isUnsupported(type);
    }

    @Override
    public boolean canConvert(DataType from, DataType to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object convert(Object value, DataType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataType commonType(DataType left, DataType right) {
        throw new UnsupportedOperationException();
    }

}
