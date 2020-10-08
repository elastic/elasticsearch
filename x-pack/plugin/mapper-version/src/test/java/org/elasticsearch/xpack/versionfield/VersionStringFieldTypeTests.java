/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;

import java.io.IOException;
import java.util.Collections;

public class VersionStringFieldTypeTests extends FieldTypeTestCase {

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new VersionStringFieldMapper.Builder("field").build(context).fieldType();
        assertEquals(Collections.singletonList("value"), fetchSourceValue(mapper, "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(mapper, 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(mapper, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(mapper, "value", "format"));
        assertEquals("Field [field] doesn't support formats.", e.getMessage());
    }
}
