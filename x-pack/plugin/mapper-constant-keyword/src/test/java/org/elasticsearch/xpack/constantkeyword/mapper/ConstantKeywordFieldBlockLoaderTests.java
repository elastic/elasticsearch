/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.constantkeyword.ConstantKeywordMapperPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ConstantKeywordFieldBlockLoaderTests extends BlockLoaderTestCase {
    public ConstantKeywordFieldBlockLoaderTests(Params params) {
        super(FieldType.CONSTANT_KEYWORD.toString(), params);
    }

    @Override
    public void testBlockLoaderOfMultiField() throws IOException {
        // Multi fields are noop for constant_keyword.
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        return new BytesRef((String) fieldMapping.get("value"));
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        return List.of(new ConstantKeywordMapperPlugin());
    }
}
