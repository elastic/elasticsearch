/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.join.mapper;

import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Defines a relationship between a parent type and a set of child types
 */
class Relations {

    final String parent;
    final Set<String> children;

    Relations(String parent, Set<String> children) {
        this.parent = parent;
        this.children = children;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Relations relation = (Relations) o;
        return Objects.equals(parent, relation.parent) &&
            Objects.equals(children, relation.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parent, children);
    }

    @Override
    public String toString() {
        return parent + "->" + children;
    }

    static List<Relations> parse(Object fieldNode) {
        List<Relations> parsed = new ArrayList<>();
        Map<String, Object> relations = XContentMapValues.nodeMapValue(fieldNode, "relations");
        for (Map.Entry<String, Object> relation : relations.entrySet()) {
            final String parent = relation.getKey();
            Set<String> children;
            if (XContentMapValues.isArray(relation.getValue())) {
                children = new HashSet<>(Arrays.asList(XContentMapValues.nodeStringArrayValue(relation.getValue())));
            } else {
                children = Collections.singleton(relation.getValue().toString());
            }
            parsed.add(new Relations(parent, children));
        }
        return parsed;
    }
}
