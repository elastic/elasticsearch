/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields.mapper;

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
