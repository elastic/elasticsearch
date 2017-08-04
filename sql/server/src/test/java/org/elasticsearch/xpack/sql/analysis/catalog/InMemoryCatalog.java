/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * In memory implementation of catalog designed for testing.
 */
class InMemoryCatalog implements Catalog {
    private final Map<String, EsIndex> indices;

    InMemoryCatalog(List<EsIndex> indices) {
        this.indices = indices.stream().collect(toMap(EsIndex::name, Function.identity()));
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
    public boolean indexIsValid(String index) {
        return true;
    }

    @Override
    public EsIndex getIndex(String index) {
        return indices.get(index);
    }

}