/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.flattened.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FlatObjectFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        Map<String, Object> sourceValue = Map.of("key", "value");
        MappedFieldType mapper = new FlatObjectFieldMapper.Builder("field").build(context).fieldType();
        assertEquals(List.of(sourceValue), fetchSourceValue(mapper, sourceValue));

        MappedFieldType nullValueMapper = new FlatObjectFieldMapper.Builder("field")
            .nullValue("NULL")
            .build(context)
            .fieldType();
        assertEquals(List.of("NULL"), fetchSourceValue(nullValueMapper, null));
    }
}
