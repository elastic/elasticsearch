/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.Version;

import java.util.Collection;

/**
 * Central class for {@link DataType} creation and conversion.
 */
public interface DataTypeRegistry {

    //
    // Discovery
    //
    Collection<DataType> dataTypes();

    DataType fromEs(String typeName);

    // version-dependent type resolution
    default DataType fromEs(String typeName, Version version) {
        return fromEs(typeName);
    }

    DataType fromJava(Object value);

    boolean isUnsupported(DataType type);

    //
    // Conversion methods
    //
    boolean canConvert(DataType from, DataType to);

    Object convert(Object value, DataType type);

    DataType commonType(DataType left, DataType right);
}
