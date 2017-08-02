/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.Types;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class EsIndex {

    public static final EsIndex NOT_FOUND = new EsIndex(StringUtils.EMPTY, emptyMap(), emptyList(), Settings.EMPTY);

    private final String name;
    private final Map<String, DataType> mapping;
    private final List<String> aliases;
    private final Settings settings;

    public EsIndex(String name, Map<String, DataType> mapping, List<String> aliases, Settings settings) {
        this.name = name;
        this.mapping = mapping;
        this.aliases = aliases;
        this.settings = settings;
    }

    public String name() {
        return name;
    }

    public Map<String, DataType> mapping() {
        return mapping;
    }

    public List<String> aliases() {
        return aliases;
    }

    public Settings settings() {
        return settings;
    }

    static List<EsIndex> build(Iterator<IndexMetaData> metadata) {
        if (metadata == null || !metadata.hasNext()) {
            return emptyList();
        }
        List<EsIndex> list = new ArrayList<>();
        while (metadata.hasNext()) {
            build(metadata.next());
        }
        return list;
    }

    static EsIndex build(IndexMetaData metadata) {
        ImmutableOpenMap<String, MappingMetaData> mappings = metadata.getMappings();
        if (mappings.size() > 1) {
            throw new SqlIllegalArgumentException("Cannot use index [%s] as it contains multiple types %s", metadata.getIndex().getName(), Arrays.toString(mappings.keys().toArray()));
        }
        MappingMetaData mm = mappings.isEmpty() ? null : mappings.valuesIt().next();
        Map<String, DataType> mapping = mm != null ? Types.fromEs(mm.sourceAsMap()) : emptyMap();

        List<String> aliases = Arrays.asList(metadata.getAliases().keys().toArray(String.class));
        return new EsIndex(metadata.getIndex().getName(), mapping, aliases, metadata.getSettings());
    }

    @Override
    public String toString() {
        return name;
    }
}
