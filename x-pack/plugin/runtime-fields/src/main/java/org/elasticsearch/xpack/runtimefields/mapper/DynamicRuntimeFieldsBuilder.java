/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.mapper.RuntimeFieldType;

public final class DynamicRuntimeFieldsBuilder implements org.elasticsearch.index.mapper.DynamicRuntimeFieldsBuilder {

    public static final DynamicRuntimeFieldsBuilder INSTANCE = new DynamicRuntimeFieldsBuilder();

    private DynamicRuntimeFieldsBuilder() {}

    @Override
    public RuntimeFieldType newDynamicStringField(String name) {
        return new KeywordScriptFieldType(name);
    }

    @Override
    public RuntimeFieldType newDynamicLongField(String name) {
        return new LongScriptFieldType(name);
    }

    @Override
    public RuntimeFieldType newDynamicDoubleField(String name) {
        return new DoubleScriptFieldType(name);
    }

    @Override
    public RuntimeFieldType newDynamicBooleanField(String name) {
        return new BooleanScriptFieldType(name);
    }

    @Override
    public RuntimeFieldType newDynamicDateField(String name, DateFormatter dateFormatter) {
        return new DateScriptFieldType(name, dateFormatter);
    }
}
