/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public final class GroupByOptimizer {

    private GroupByOptimizer() {}

    /**
     * Returns an ordered collection of group by fields in order to get better performance.
     *
     * The decision is based on the type and whether the input field is a indexed/runtime/script field
     *
     * TODO: take index sorting into account
     *
     * @param groups group by as defined by the user
     * @param runtimeFields set of runtime fields
     * @return collection in order of priority
     */
    static Collection<Entry<String, SingleGroupSource>> reorderGroups(Map<String, SingleGroupSource> groups, Set<String> runtimeFields) {
        if (groups.size() == 1) {
            return groups.entrySet();
        }

        List<Tuple<Entry<String, SingleGroupSource>, Integer>> prioritizedGroups = new ArrayList<>(groups.size());

        // respect the order in the configuration by giving every entry a base priority
        int basePriority = groups.size();

        for (Entry<String, SingleGroupSource> groupBy : groups.entrySet()) {
            // prefer indexed fields over runtime fields over scripts
            int priority = basePriority-- + (groupBy.getValue().getScriptConfig() == null
                ? runtimeFields.contains(groupBy.getValue().getField()) ? 250 : 500
                : 0);

            switch (groupBy.getValue().getType()) {
                case DATE_HISTOGRAM:
                    priority += 4000;
                    break;
                case HISTOGRAM:
                    priority += 3000;
                    break;
                case TERMS:
                    priority += 2000;
                    break;
                case GEOTILE_GRID:
                    priority += 1000;
                    break;
                default:
                    assert false : "new group source type misses priority definition";
            }

            prioritizedGroups.add(new Tuple<>(groupBy, priority));
        }

        prioritizedGroups.sort(Comparator.comparing(Tuple<Entry<String, SingleGroupSource>, Integer>::v2).reversed());

        return prioritizedGroups.stream().map(x -> x.v1()).collect(Collectors.toList());
    }
}
