/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public abstract class InMemoryCatalog implements Catalog {

    protected final Map<String, EsIndex> indices = new LinkedHashMap<>();
    protected final Map<String, Map<String, EsType>> types = new LinkedHashMap<>();

    {
        List<EsIndex> idxs = indices();
        for (EsIndex esIndex : idxs) {
            indices.put(esIndex.name(), esIndex);
            types.put(esIndex.name(), new LinkedHashMap<>());
        }

        List<EsType> tps = types();
        for (EsType esType : tps) {
            Map<String, EsType> idxTypes = types.get(esType.index());
            idxTypes.put(esType.name(), esType);
        }
    }

    @Override
    public EsIndex getIndex(String index) {
        EsIndex idx = indices.get(index);
        return (idx == null ? EsIndex.NOT_FOUND : idx);
    }

    @Override
    public boolean indexExists(String index) {
        return indices.containsKey(index);
    }

    @Override
    public List<EsIndex> listIndices() {
        return new ArrayList<>(indices.values());
    }

    @Override
    public List<EsIndex> listIndices(String pattern) {
        Pattern p = StringUtils.likeRegex(pattern);
        return indices.entrySet().stream()
            .filter(e -> p.matcher(e.getKey()).matches())
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
    }

    @Override
    public EsType getType(String index, String type) {
        Map<String, EsType> typs = types.get(index);
        if (typs == null) {
            return EsType.NOT_FOUND;
        }
        EsType esType = typs.get(type);
        return (esType == null ? EsType.NOT_FOUND : esType);
    }

    @Override
    public boolean typeExists(String index, String type) {
        Map<String, EsType> typs = types.get(index);
        return (typs != null && typs.containsKey(type));
    }

    @Override
    public List<EsType> listTypes(String index) {
        Map<String, EsType> typs = types.get(index);
        return typs != null ? new ArrayList<>(typs.values()) : emptyList();
    }

    @Override
    public List<EsType> listTypes(String index, String pattern) {
        Map<String, EsType> typs = types.get(index);
        if (typs == null) {
            return emptyList();
        }

        Pattern p = StringUtils.likeRegex(pattern);
        return typs.entrySet().stream()
                .filter(e -> p.matcher(e.getKey()).matches())
                .map(Map.Entry::getValue)
                .collect(toList());
    }

    public void clear() {
        indices.clear();
        types.clear();
    }

    protected abstract List<EsIndex> indices();

    protected abstract List<EsType> types();
}