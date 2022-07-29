/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.mapper;

import org.elasticsearch.common.xcontent.support.XContentMapValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines a relationship between a parent type and a set of child types
 */
record Relations(String parent, Set<String> children) {

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
