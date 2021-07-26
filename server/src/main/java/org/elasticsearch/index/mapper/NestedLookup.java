/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Holds information about nested mappings
 */
public interface NestedLookup {

    /**
     * @return a map of all nested object mappers in the current mapping
     */
    Map<String, NestedObjectMapper> getNestedMappers();

    /**
     * @return filters for nested objects that contain further nested mappers
     */
    Map<String, Query> getNestedParentFilters();

    /**
     * Given a nested object path, returns the path to its nested parent
     *
     * In particular, if a nested field `foo` contains an object field
     * `bar.baz`, then calling this method with `foo.bar.baz` will return
     * the path `foo`, skipping over the object-but-not-nested `foo.bar`
     *
     * @param path the path to resolve
     */
    String getNestedParent(String path);

    /**
     * A NestedLookup for a mapping with no nested mappers
     */
    NestedLookup EMPTY = new NestedLookup() {
        @Override
        public Map<String, NestedObjectMapper> getNestedMappers() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, Query> getNestedParentFilters() {
            return Collections.emptyMap();
        }

        @Override
        public String getNestedParent(String path) {
            return null;
        }
    };

    /**
     * Construct a NestedLookup from a list of NestedObjectMappers
     * @param mappers   the nested mappers to build a lookup over
     */
    static NestedLookup build(List<NestedObjectMapper> mappers) {
        if (mappers == null || mappers.isEmpty()) {
            return NestedLookup.EMPTY;
        }
        mappers = mappers.stream().sorted(Comparator.comparing(ObjectMapper::name)).collect(Collectors.toList());
        Map<String, Query> parentFilters = new HashMap<>();
        Map<String, NestedObjectMapper> mappersByName = new HashMap<>();
        NestedObjectMapper previous = null;
        for (NestedObjectMapper mapper : mappers) {
            mappersByName.put(mapper.name(), mapper);
            if (previous == null) {
                previous = mapper;
            } else {
                if (mapper.name().startsWith(previous.name() + ".")) {
                    parentFilters.put(previous.name(), previous.nestedTypeFilter());
                }
            }
        }
        List<String> nestedPathNames = mappers.stream()
            .map(NestedObjectMapper::name)
            .collect(Collectors.toList());
        return new NestedLookup() {

            @Override
            public Map<String, NestedObjectMapper> getNestedMappers() {
                return mappersByName;
            }

            @Override
            public Map<String, Query> getNestedParentFilters() {
                return parentFilters;
            }

            @Override
            public String getNestedParent(String path) {
                String parent = null;
                for (String parentPath : nestedPathNames) {
                    if (path.startsWith(parentPath) && path.equals(parentPath) == false) {
                        // path names are ordered so this will give us the
                        // parent with the longest path
                        parent = parentPath;
                    }
                }
                return parent;
            }
        };
    }
}
