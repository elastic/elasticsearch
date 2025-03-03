/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import java.util.HashMap;
import java.util.Map;

public record Mapping(Map<String, Object> mapping) {

    public static Mapping generate(Template template) {
        var mapping = new HashMap<String, Object>();

        var topLevel = new HashMap<String, Object>();
        generate(topLevel, template.template());

        mapping.put("_doc", Map.of("properties", topLevel));
        return new Mapping(mapping);
    }

    private static void generate(Map<String, Object> mapping, Map<String, Template.Entry> template) {
        for (var entry : template.values()) {
            if (entry instanceof Template.Leaf l) {
                mapping.put(l.name(), Map.of("type", l.type().toString()));
                continue;
            }
            if (entry instanceof Template.Object o) {
                var children = new HashMap<String, Object>();
                mapping.put(o.name(), Map.of("properties", children));

                generate(children, o.children());
            }
        }
    }
}
