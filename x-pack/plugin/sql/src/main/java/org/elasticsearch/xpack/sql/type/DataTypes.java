/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.type;

import org.elasticsearch.xpack.ql.expression.literal.Interval;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoShape;

public final class DataTypes {

    private DataTypes() {}

    public static DataType fromJava(Object value) {
        if (value instanceof Interval) {
            return ((Interval<?>) value).dataType();
        }

        if (value instanceof GeoShape) {
            return DataType.GEO_SHAPE;
        }

        return DataTypes.fromJava(value);
    }
}
