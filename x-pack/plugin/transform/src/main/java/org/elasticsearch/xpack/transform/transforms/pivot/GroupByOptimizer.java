/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public final class GroupByOptimizer {

    private static class UnmodifiableEntryWithPriority implements Entry<String, SingleGroupSource> {

        private final Entry<String, SingleGroupSource> entry;
        private final int priority;

        UnmodifiableEntryWithPriority(Entry<String, SingleGroupSource> entry, int priority) {
            this.entry = entry;
            this.priority = priority;
        }

        public int compareTo(UnmodifiableEntryWithPriority o) {
            return Integer.compare(priority, o.priority);
        }

        @Override
        public String getKey() {
            return entry.getKey();
        }

        @Override
        public SingleGroupSource getValue() {
            return entry.getValue();
        }

        @Override
        public SingleGroupSource setValue(SingleGroupSource value) {
            // not modifiable
            throw new UnsupportedOperationException();
        }
    }

    private GroupByOptimizer() {}

    /**
     * Returns an ordered collection of group by fields in order to get better performance.
     *
     * The decision is based on the type and whether the input field is a indexed/runtime/script field
     *
     * TODO: take index sorting into account
     *
     * @param groups group by as defined by the user
     * @param runtimeFields set of strings which fields are runtime fields
     * @return collection in order of priority
     */
    static Collection<Entry<String, SingleGroupSource>> reorderGroups(Map<String, SingleGroupSource> groups, Set<String> runtimeFields) {
        if (groups.size() == 1) {
            return groups.entrySet();
        }

        // Arrays.sort provides stable sort (to respect the input order if priorities match), Collections.sort not
        // as generic arrays and generic types can't be combined, the "struct" is necessary
        UnmodifiableEntryWithPriority[] prioritizedGroups = new UnmodifiableEntryWithPriority[groups.size()];

        int index = 0;
        for (Entry<String, SingleGroupSource> groupBy : groups.entrySet()) {
            // prefer indexed fields over runtime fields over scripts
            int priority = groupBy.getValue().getScriptConfig() == null
                ? runtimeFields.contains(groupBy.getValue().getField()) ? 25 : 50
                : 0;

            switch (groupBy.getValue().getType()) {
                case DATE_HISTOGRAM:
                    priority += 400;
                    break;
                case HISTOGRAM:
                    priority += 300;
                    break;
                case TERMS:
                    priority += 200;
                    break;
                case GEOTILE_GRID:
                    priority += 100;
                    break;
                default:
                    assert false : "new group source type misses priority definition";
            }

            prioritizedGroups[index++] = new UnmodifiableEntryWithPriority(groupBy, priority);
        }

        Arrays.sort(prioritizedGroups, (a, b) -> b.compareTo(a));
        return Arrays.asList(prioritizedGroups);
    }
}
