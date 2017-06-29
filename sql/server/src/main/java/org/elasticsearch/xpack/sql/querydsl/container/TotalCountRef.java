/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.util.StringUtils;

// somewhat of a fake agg (since it gets optimized and it gets its value from the response)
public final class TotalCountRef extends AggRef {
    public static TotalCountRef INSTANCE = new TotalCountRef();

    TotalCountRef() {
        super(StringUtils.EMPTY);
    }

    @Override
    public String toString() {
        return "TotalCountRef";
    }
}