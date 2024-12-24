/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.util.Collection;
import java.util.Collections;

public class SemanticInferenceMetadataFieldMapperTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return Collections.singletonList(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    public void testFieldHasValue() {
        assertTrue(
            getMappedFieldType().fieldHasValue(
                new FieldInfos(new FieldInfo[] { getFieldInfoWithName(SemanticInferenceMetadataFieldsMapper.NAME) })
            )
        );
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        assertFalse(getMappedFieldType().fieldHasValue(FieldInfos.EMPTY));
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return new SemanticInferenceMetadataFieldsMapper.FieldType();
    }
}
