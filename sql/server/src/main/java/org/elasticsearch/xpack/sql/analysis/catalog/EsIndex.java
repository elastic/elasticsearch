/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.Map;

import static java.util.Collections.emptyMap;

public class EsIndex {

    public static final EsIndex NOT_FOUND = new EsIndex(StringUtils.EMPTY, emptyMap());

    private final String name;
    private final Map<String, DataType> mapping;

    public EsIndex(String name, Map<String, DataType> mapping) {
        assert name != null;
        assert mapping != null;
        this.name = name;
        this.mapping = mapping;
    }

    public String name() {
        return name;
    }

    public Map<String, DataType> mapping() {
        return mapping;
    }

    @Override
    public String toString() {
        return name;
    }
}
