/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * RuntimeField base class for leaf fields that will only ever return
 * a single MappedFieldType from {@link RuntimeField#asMappedFieldTypes()}
 */
public final class LeafRuntimeField implements RuntimeField {
    private final String name;
    private final ToXContent toXContent;
    private final MappedFieldType mappedFieldType;

    public LeafRuntimeField(String name, MappedFieldType mappedFieldType, ToXContent toXContent) {
        this.name = name;
        this.toXContent = toXContent;
        this.mappedFieldType = mappedFieldType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String typeName() {
        return mappedFieldType.typeName();
    }

    @Override
    public Collection<MappedFieldType> asMappedFieldTypes() {
        return Collections.singleton(mappedFieldType);
    }

    @Override
    public void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        toXContent.toXContent(builder, params);
    }
}
