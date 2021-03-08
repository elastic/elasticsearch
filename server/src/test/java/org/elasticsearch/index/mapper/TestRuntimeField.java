/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

public class TestRuntimeField implements RuntimeFieldType {

    private final String type;
    private final String name;
    private final Supplier<MappedFieldType> fieldType;

    public TestRuntimeField(String name, String type, Supplier<MappedFieldType> fieldType) {
        this.name = name;
        this.type = type;
        this.fieldType = fieldType;
    }

    @Override
    public String typeName() {
        return type;
    }

    @Override
    public MappedFieldType asMappedFieldType(Function<String, MappedFieldType> lookup) {
        return fieldType.get();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", type);
        builder.endObject();
        return builder;
    }
}
