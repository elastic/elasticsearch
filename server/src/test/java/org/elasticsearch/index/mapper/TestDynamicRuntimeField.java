/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class TestDynamicRuntimeField implements RuntimeField, DynamicFieldType {

    private final String name;
    private final Map<String, MappedFieldType> subfields;

    public TestDynamicRuntimeField(String name) {
        this(name, Collections.emptyMap());
    }

    public TestDynamicRuntimeField(String name, Map<String, MappedFieldType> subfields) {
        this.name = name;
        this.subfields = subfields;
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) {

    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String typeName() {
        return "dynamic";
    }

    @Override
    public MappedFieldType asMappedFieldType() {
        return null;
    }

    @Override
    public MappedFieldType getChildFieldType(String path) {
        if (subfields.containsKey(path)) {
            return subfields.get(path);
        }
        return new KeywordScriptFieldType(name + "." + path);
    }

    @Override
    public Set<String> getKnownSubfields() {
        return subfields.keySet();
    }
}
