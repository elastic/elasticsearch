/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;

import java.io.IOException;
import java.util.Map;

public class CustomSyntheticSourceFieldLookupTests extends MapperServiceTestCase {
    private static String MAPPING = """
        {
          "_doc": {
            "properties": {
              "keep_all": {
                "type": "keyword",
                "synthetic_source_keep": "all"
              },
              "keep_arrays": {
                "type": "keyword",
                "synthetic_source_keep": "arrays"
              },
              "fallback_impl": {
                "type": "long",
                "doc_values": "false"
              },
              "object_keep_all": {
                "properties": {},
                "synthetic_source_keep": "all"
              },
              "object_keep_arrays": {
                "properties": {},
                "synthetic_source_keep": "arrays"
              },
              "object_disabled": {
                "properties": {},
                "enabled": "false"
              },
              "nested_keep_all": {
                "type": "nested",
                "properties": {},
                "synthetic_source_keep": "all"
              },
              "nested_disabled": {
                "type": "nested",
                "properties": {},
                "enabled": "false"
              },
              "just_field": {
                "type": "boolean"
              },
              "just_object": {
                "properties": {}
              },
              "nested_obj": {
                "properties": {
                  "keep_all": {
                    "type": "keyword",
                    "synthetic_source_keep": "all"
                  },
                  "keep_arrays": {
                    "type": "keyword",
                    "synthetic_source_keep": "arrays"
                  },
                  "fallback_impl": {
                    "type": "long",
                    "doc_values": "false"
                  },
                  "object_keep_all": {
                    "properties": {},
                    "synthetic_source_keep": "all"
                  },
                  "object_keep_arrays": {
                    "properties": {},
                    "synthetic_source_keep": "arrays"
                  },
                  "object_disabled": {
                    "properties": {},
                    "enabled": "false"
                  },
                  "nested_keep_all": {
                    "type": "nested",
                    "properties": {},
                    "synthetic_source_keep": "all"
                  },
                  "nested_disabled": {
                    "type": "nested",
                    "properties": {},
                    "enabled": "false"
                  },
                  "just_field": {
                    "type": "boolean"
                  },
                  "just_object": {
                    "properties": {}
                  }
                }
              },
              "nested_nested": {
                "properties": {
                  "keep_all": {
                    "type": "keyword",
                    "synthetic_source_keep": "all"
                  },
                  "keep_arrays": {
                    "type": "keyword",
                    "synthetic_source_keep": "arrays"
                  },
                  "fallback_impl": {
                    "type": "long",
                    "doc_values": "false"
                  },
                  "object_keep_all": {
                    "properties": {},
                    "synthetic_source_keep": "all"
                  },
                  "object_keep_arrays": {
                    "properties": {},
                    "synthetic_source_keep": "arrays"
                  },
                  "object_disabled": {
                    "properties": {},
                    "enabled": "false"
                  },
                  "nested_keep_all": {
                    "type": "nested",
                    "properties": {},
                    "synthetic_source_keep": "all"
                  },
                  "nested_disabled": {
                    "type": "nested",
                    "properties": {},
                    "enabled": "false"
                  },
                  "just_field": {
                    "type": "boolean"
                  },
                  "just_object": {
                    "properties": {}
                  }
                }
              }
            }
          }
        }
        """;

    public void testIsNoopWhenSourceIsNotSynthetic() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.NONE);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, false);

        assertEquals(sut.getFieldsWithCustomSyntheticSourceHandling(), Map.of());
    }

    public void testDetectsLeafWithKeepAll() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.NONE);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, true);

        var fields = sut.getFieldsWithCustomSyntheticSourceHandling();
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("keep_all"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("nested_obj.keep_all"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("nested_nested.keep_all"));
    }

    public void testDetectsLeafWithKeepArrays() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.NONE);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, true);

        var fields = sut.getFieldsWithCustomSyntheticSourceHandling();
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("keep_arrays"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_obj.keep_arrays"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_nested.keep_arrays"));
    }

    public void testDetectsLeafWithFallback() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.NONE);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, true);

        var fields = sut.getFieldsWithCustomSyntheticSourceHandling();
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.FALLBACK_SYNTHETIC_SOURCE, fields.get("fallback_impl"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.FALLBACK_SYNTHETIC_SOURCE, fields.get("nested_obj.fallback_impl"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.FALLBACK_SYNTHETIC_SOURCE, fields.get("nested_nested.fallback_impl"));
    }

    public void testDetectsObjectWithKeepAll() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.NONE);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, true);

        var fields = sut.getFieldsWithCustomSyntheticSourceHandling();

        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("object_keep_all"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("nested_obj.object_keep_all"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("nested_nested.object_keep_all"));

        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("nested_keep_all"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("nested_obj.nested_keep_all"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ALL, fields.get("nested_nested.nested_keep_all"));
    }

    public void testDetectsObjectWithKeepArrays() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.NONE);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, true);

        var fields = sut.getFieldsWithCustomSyntheticSourceHandling();
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("object_keep_arrays"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_obj.object_keep_arrays"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_nested.object_keep_arrays"));
    }

    public void testDetectsDisabledObject() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.NONE);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, true);

        var fields = sut.getFieldsWithCustomSyntheticSourceHandling();

        assertEquals(CustomSyntheticSourceFieldLookup.Reason.DISABLED_OBJECT, fields.get("object_disabled"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.DISABLED_OBJECT, fields.get("nested_obj.object_disabled"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.DISABLED_OBJECT, fields.get("nested_nested.object_disabled"));

        assertEquals(CustomSyntheticSourceFieldLookup.Reason.DISABLED_OBJECT, fields.get("nested_disabled"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.DISABLED_OBJECT, fields.get("nested_obj.nested_disabled"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.DISABLED_OBJECT, fields.get("nested_nested.nested_disabled"));
    }

    public void testAppliesIndexLevelSourceKeepMode() throws IOException {
        var mapping = createMapperService(MAPPING).mappingLookup().getMapping();
        var indexSettings = indexSettings(Mapper.SourceKeepMode.ARRAYS);
        var sut = new CustomSyntheticSourceFieldLookup(mapping, indexSettings, true);

        var fields = sut.getFieldsWithCustomSyntheticSourceHandling();

        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("just_field"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_obj.just_field"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_nested.just_field"));

        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("just_object"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_obj.just_object"));
        assertEquals(CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS, fields.get("nested_nested.just_object"));
    }

    private static IndexSettings indexSettings(Mapper.SourceKeepMode sourceKeepMode) {
        return createIndexSettings(
            IndexVersion.current(),
            Settings.builder().put(Mapper.SYNTHETIC_SOURCE_KEEP_INDEX_SETTING.getKey(), sourceKeepMode).build()
        );
    }
}
