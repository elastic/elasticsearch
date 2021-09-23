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
import java.util.List;
import java.util.stream.Stream;

/**
 * RuntimeField base class for leaf fields that will only ever return a single {@link MappedFieldType}
 * from {@link RuntimeField#asMappedFieldTypes()}. Can be a standalone runtime field, or part of a composite.
 */
public final class LeafRuntimeField implements RuntimeField {
    private final String name;
    private final MappedFieldType mappedFieldType;
    private final List<FieldMapper.Parameter<?>> parameters;

    public LeafRuntimeField(String name, MappedFieldType mappedFieldType, List<FieldMapper.Parameter<?>> parameters) {
        this.name = name;
        this.mappedFieldType = mappedFieldType;
        this.parameters = parameters;
        assert mappedFieldType.name().endsWith(name) : "full name: " + mappedFieldType.name() + " - leaf name: " + name;
    }

    @Override
    public String name() {
        return mappedFieldType.name();
    }

    @Override
    public Stream<MappedFieldType> asMappedFieldTypes() {
        return Stream.of(mappedFieldType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", mappedFieldType.typeName());
        boolean includeDefaults = params.paramAsBoolean("include_defaults", false);
        for (FieldMapper.Parameter<?> parameter : parameters) {
            parameter.toXContent(builder, includeDefaults);
        }
        builder.endObject();
        return builder;
    }
}
