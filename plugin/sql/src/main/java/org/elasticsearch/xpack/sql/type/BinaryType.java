/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;

public class BinaryType extends AbstractDataType {

    BinaryType(boolean docValues) {
        super(JDBCType.VARBINARY, docValues);
    }

    @Override
    public String esName() {
        return "binary";
    }
}
