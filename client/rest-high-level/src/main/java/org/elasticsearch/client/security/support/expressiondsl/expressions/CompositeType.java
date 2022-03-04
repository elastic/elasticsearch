/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl.expressions;

import org.elasticsearch.xcontent.ParseField;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum CompositeType {

    ANY("any"),
    ALL("all"),
    EXCEPT("except");

    private static Map<String, CompositeType> nameToType = Collections.unmodifiableMap(initialize());
    private ParseField field;

    CompositeType(String name) {
        this.field = new ParseField(name);
    }

    public String getName() {
        return field.getPreferredName();
    }

    public ParseField getParseField() {
        return field;
    }

    public static CompositeType fromName(String name) {
        return nameToType.get(name);
    }

    private static Map<String, CompositeType> initialize() {
        Map<String, CompositeType> map = new HashMap<>();
        for (CompositeType field : values()) {
            map.put(field.getName(), field);
        }
        return map;
    }

}
