/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NestedLookup {

    private final List<NestedObjectMapper> mappers;
    private final Map<String, Query> parentFilters = new HashMap<>();

    public static NestedLookup build(List<NestedObjectMapper> mappers) {
        if (mappers == null || mappers.isEmpty()) {
            return null;
        }
        return new NestedLookup(mappers);
    }

    private NestedLookup(List<NestedObjectMapper> mappers) {
        assert mappers.isEmpty() == false;
        this.mappers = mappers;
        this.mappers.sort(Comparator.comparing(ObjectMapper::name));

        NestedObjectMapper previous = null;
        for (NestedObjectMapper mapper : mappers) {
            if (previous == null) {
                previous = mapper;
            } else {
                if (mapper.name().startsWith(previous.name() + ".")) {
                    parentFilters.put(previous.name(), previous.nestedTypeFilter());
                }
            }
        }
    }

    public Map<String, Query> getNestedParentFilters() {
        return parentFilters;
    }
}
