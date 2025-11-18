/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SubGroup implements ToXContentFragment {
    private final String name;
    private Long count;
    @UpdateForV10(owner = UpdateForV10.Owner.PROFILING) // remove legacy XContent rendering
    private final Map<String, SubGroup> subgroups;

    public static SubGroup root(String name) {
        return new SubGroup(name, null, new HashMap<>());
    }

    public SubGroup(String name, Long count, Map<String, SubGroup> subgroups) {
        this.name = name;
        this.count = count;
        this.subgroups = subgroups;
    }

    public SubGroup addCount(String name, long count) {
        if (this.subgroups.containsKey(name) == false) {
            this.subgroups.put(name, new SubGroup(name, count, new HashMap<>()));
        } else {
            SubGroup s = this.subgroups.get(name);
            s.count += count;
        }
        return this;
    }

    public SubGroup getOrAddChild(String name) {
        if (subgroups.containsKey(name) == false) {
            this.subgroups.put(name, new SubGroup(name, null, new HashMap<>()));
        }
        return this.subgroups.get(name);
    }

    public Long getCount(String name) {
        SubGroup subGroup = this.subgroups.get(name);
        return subGroup != null ? subGroup.count : null;
    }

    public SubGroup getSubGroup(String name) {
        return this.subgroups.get(name);
    }

    public SubGroup copy() {
        Map<String, SubGroup> copy = new HashMap<>(subgroups.size());
        for (Map.Entry<String, SubGroup> subGroup : subgroups.entrySet()) {
            copy.put(subGroup.getKey(), subGroup.getValue().copy());
        }
        return new SubGroup(name, count, copy);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        // only the root node has no count
        if (count != null) {
            builder.field("count", count);
        }
        if (subgroups != null && subgroups.isEmpty() == false) {
            for (SubGroup subgroup : subgroups.values()) {
                subgroup.toXContent(builder, params);
            }
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubGroup subGroup = (SubGroup) o;
        return Objects.equals(name, subGroup.name)
            && Objects.equals(count, subGroup.count)
            && Objects.equals(subgroups, subGroup.subgroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, count, subgroups);
    }

    @Override
    public String toString() {
        return name;
    }

    public void merge(SubGroup s) {
        if (s == null) {
            return;
        }
        // must have the same name
        if (this.name.equals(s.name)) {
            if (this.count != null && s.count != null) {
                this.count += s.count;
            } else if (this.count == null) {
                this.count = s.count;
            }
            for (SubGroup subGroup : s.subgroups.values()) {
                if (this.subgroups.containsKey(subGroup.name)) {
                    // merge
                    this.subgroups.get(subGroup.name).merge(subGroup);
                } else {
                    // add sub group as is (recursively)
                    this.subgroups.put(subGroup.name, subGroup.copy());
                }
            }
        }
    }
}
