/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.contains;

public class DocumentParserContextTests extends ESTestCase {

    private TestDocumentParserContext context = new TestDocumentParserContext();
    private final MapperBuilderContext root = MapperBuilderContext.root(false, false);

    public void testDynamicMapperSizeMultipleMappers() {
        context.addDynamicMapper(new TextFieldMapper.Builder("foo", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(1, context.getNewFieldsSize());
        context.addDynamicMapper(new TextFieldMapper.Builder("bar", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(2, context.getNewFieldsSize());
        context.addDynamicRuntimeField(new TestRuntimeField("runtime1", "keyword"));
        assertEquals(3, context.getNewFieldsSize());
        context.addDynamicRuntimeField(new TestRuntimeField("runtime2", "keyword"));
        assertEquals(4, context.getNewFieldsSize());
    }

    public void testDynamicMapperSizeSameFieldMultipleRuntimeFields() {
        context.addDynamicRuntimeField(new TestRuntimeField("foo", "keyword"));
        context.addDynamicRuntimeField(new TestRuntimeField("foo", "keyword"));
        assertEquals(context.getNewFieldsSize(), 1);
    }

    public void testDynamicMapperSizeSameFieldMultipleMappers() {
        context.addDynamicMapper(new TextFieldMapper.Builder("foo", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(1, context.getNewFieldsSize());
        context.addDynamicMapper(new TextFieldMapper.Builder("foo", createDefaultIndexAnalyzers(), false).build(root));
        assertEquals(1, context.getNewFieldsSize());
    }

    public void testAddRuntimeFieldWhenLimitIsReachedViaMapper() {
        context = new TestDocumentParserContext(
            Settings.builder()
                .put("index.mapping.total_fields.limit", 1)
                .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
                .build()
        );
        assertTrue(context.addDynamicMapper(new KeywordFieldMapper.Builder("keyword_field", IndexVersion.current()).build(root)));
        assertFalse(context.addDynamicRuntimeField(new TestRuntimeField("runtime_field", "keyword")));
        assertThat(context.getIgnoredFields(), contains("runtime_field"));
    }

    public void testAddFieldWhenLimitIsReachedViaRuntimeField() {
        context = new TestDocumentParserContext(
            Settings.builder()
                .put("index.mapping.total_fields.limit", 1)
                .put("index.mapping.total_fields.ignore_dynamic_beyond_limit", true)
                .build()
        );
        assertTrue(context.addDynamicRuntimeField(new TestRuntimeField("runtime_field", "keyword")));
        assertFalse(context.addDynamicMapper(new KeywordFieldMapper.Builder("keyword_field", IndexVersion.current()).build(root)));
        assertThat(context.getIgnoredFields(), contains("keyword_field"));
    }

}
