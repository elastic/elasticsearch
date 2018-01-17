/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;

public class IpType extends AbstractDataType {

    IpType(boolean docValues) {
        super(JDBCType.VARCHAR, docValues);
    }

    @Override
    public String esName() {
        return "ip";
    }

    @Override
    public int precision() {
        // maximum address in IPv6
        return 39;
    }
}
