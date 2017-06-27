/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.Types;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

public class EsType {

    public static final EsType NOT_FOUND = new EsType("", "", Collections.emptyMap());

    private final String index;
    private final String name;
    private final Map<String, DataType> mapping;

    public EsType(String index, String name, Map<String, DataType> mapping) {
        this.index = index;
        this.name = name;
        this.mapping = mapping;
    }

    public String index() {
        return index;
    }

    public String name() {
        return name;
    }
    
    public Map<String, DataType> mapping() {
        return mapping;
    }

    static EsType build(String index, String type, MappingMetaData metaData) {
        Map<String, Object> asMap;
        try {
            asMap = metaData.sourceAsMap();
        } catch (IOException ex) {
            throw new MappingException("Cannot get mapping info", ex);
        }

        Map<String, DataType> mapping = Types.fromEs(asMap);
        return new EsType(index, type, mapping);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("index=");
        sb.append(index);
        sb.append(",type=");
        sb.append(name);
        return sb.toString();
    }
}