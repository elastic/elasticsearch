/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;

public class GeoPointType extends AbstractDataType {

    GeoPointType() {
        super(JDBCType.NULL, false);
    }

    @Override
    public String esName() {
        return "geo-point";
    }

    @Override
    public JDBCType sqlType() {
        throw new UnsupportedOperationException("need to determine actual format");
    }
}
