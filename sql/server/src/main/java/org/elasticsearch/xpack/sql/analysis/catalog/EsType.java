/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.Types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
        Map<String, Object> asMap = metaData.sourceAsMap();

        Map<String, DataType> mapping = Types.fromEs(asMap);
        return new EsType(index, type, mapping);
    }

    static Collection<EsType> build(String index, ImmutableOpenMap<String, MappingMetaData> mapping) {
        List<EsType> tps = new ArrayList<>();

        for (ObjectObjectCursor<String, MappingMetaData> entry : mapping) {
            tps.add(build(index, entry.key, entry.value));
        }

        return tps;
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