/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ScriptFieldMapperTests extends ESSingleNodeTestCase {

    private static final String[] SUPPORTED_RUNTIME_TYPES = new String[]{"keyword"};

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, RuntimeFields.class, PainlessPlugin.class);
    }

    public void testRuntimeTypeIsRequired() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "script")
            .field("script", "value('test')")
            .endObject()
            .endObject()
            .endObject().endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: runtime_type must be specified for script field [my_field]", exception.getMessage());
    }

    public void testScriptIsRequired() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("my_field")
            .field("type", "script")
            .field("runtime_type", randomFrom(SUPPORTED_RUNTIME_TYPES))
            .endObject()
            .endObject()
            .endObject().endObject();

        MapperParsingException exception = expectThrows(MapperParsingException.class, () -> createIndex("test", Settings.EMPTY, mapping));
        assertEquals("Failed to parse mapping: script must be specified for script field [my_field]", exception.getMessage());
    }

    public void testDefaultMapping() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "script")
            .field("runtime_type", randomFrom(SUPPORTED_RUNTIME_TYPES))
            //TODO what do we need to do to make the value function work in this test?
            .field("script", "value('test')")
            .endObject()
            .endObject()
            .endObject().endObject();

        MapperService mapperService = createIndex("test", Settings.EMPTY, mapping).mapperService();
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertThat(mapper, instanceOf(ScriptFieldMapper.class));
        assertEquals(Strings.toString(mapping), Strings.toString(mapperService.documentMapper()));
    }

}
