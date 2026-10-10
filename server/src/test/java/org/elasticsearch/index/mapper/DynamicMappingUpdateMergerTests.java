/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class DynamicMappingUpdateMergerTests extends MapperServiceTestCase {

    public void testSingleUpdateIsReturnedAsIs() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        CompressedXContent update = dynamicUpdate(mapperService, """
            {"a":1}""");
        assertThat(mapperService.dynamicMappingUpdateMerger(update).merged(), sameInstance(update));
    }

    /**
     * Applying the combined update gives the same mapping as applying the update of each document one after the other.
     */
    public void testMergedUpdateIsEquivalentToSequentialUpdates() throws IOException {
        String[] docs = new String[] { """
            {"a":1}""", """
            {"b":"text","a":2}""", """
            {"obj":{"c":true}}""", """
            {"obj":{"d":1.5},"e":"2024-01-01"}""" };

        MapperService merged = createMapperService(mapping(b -> {}));
        DynamicMappingUpdateMerger merger = merged.dynamicMappingUpdateMerger(dynamicUpdate(merged, docs[0]));
        for (int i = 1; i < docs.length; i++) {
            assertTrue(merger.add(dynamicUpdate(merged, docs[i])));
        }
        merged.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);

        MapperService sequential = createMapperService(mapping(b -> {}));
        for (String doc : docs) {
            CompressedXContent update = dynamicUpdate(sequential, doc);
            if (update != null) {
                sequential.merge(MapperService.SINGLE_MAPPING_NAME, update, MergeReason.MAPPING_AUTO_UPDATE);
            }
        }
        assertThat(merged.documentMapper().mappingSource(), equalTo(sequential.documentMapper().mappingSource()));
    }

    /**
     * Each document adds one metric below an object that the index mapping already defines, mapped by a dynamic template.
     */
    public void testMergedUpdateWithDynamicTemplates() throws IOException {
        String mapping = """
            {
              "_doc": {
                "dynamic_templates": [
                  { "counter": { "path_match": "metrics.*_total", "mapping": { "type": "double", "meta": { "kind": "counter" } } } },
                  { "gauge": { "path_match": "metrics.*", "mapping": { "type": "double" } } }
                ],
                "properties": {
                  "metrics": { "type": "object", "subobjects": false }
                }
              }
            }""";
        int numDocs = between(2, 50);
        MapperService merged = createMapperService(mapping);
        MapperService sequential = createMapperService(mapping);
        DynamicMappingUpdateMerger merger = null;
        for (int i = 0; i < numDocs; i++) {
            String doc = "{\"metrics\":{\"metric." + i + (randomBoolean() ? "_total" : "") + "\":" + i + "}}";
            CompressedXContent update = dynamicUpdate(merged, doc);
            if (merger == null) {
                merger = merged.dynamicMappingUpdateMerger(update);
            } else {
                assertTrue(merger.add(update));
            }
            sequential.merge(MapperService.SINGLE_MAPPING_NAME, dynamicUpdate(sequential, doc), MergeReason.MAPPING_AUTO_UPDATE);
        }
        merged.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(merged.documentMapper().mappingSource(), equalTo(sequential.documentMapper().mappingSource()));
    }

    /**
     * An update that maps a field differently than a previous one is rejected as a whole, including the fields that don't conflict,
     * and closes the merger.
     */
    public void testConflictingUpdateIsRejected() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(dynamicUpdate(mapperService, """
            {"a":1}"""));
        assertTrue(merger.add(dynamicUpdate(mapperService, """
            {"b":1}""")));
        assertFalse(merger.add(dynamicUpdate(mapperService, """
            {"c":1,"a":"text"}""")));
        assertFalse(merger.add(dynamicUpdate(mapperService, """
            {"d":1}""")));

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(mapperService.fieldType("a").typeName(), equalTo("long"));
        assertThat(mapperService.fieldType("b"), notNullValue());
        assertThat(mapperService.fieldType("c"), nullValue());
        assertThat(mapperService.fieldType("d"), nullValue());
    }

    /**
     * The first definition of a field is the one that counts. An update that only differs by a parameter that can be changed on
     * an existing field would replace it, so it is rejected.
     */
    public void testUpdateWithDifferentParameterIsRejected() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(new CompressedXContent("""
            {"_doc":{"properties":{"a":{"type":"keyword","ignore_above":10}}}}"""));
        assertTrue(merger.add(new CompressedXContent("""
            {"_doc":{"properties":{"a":{"type":"keyword","ignore_above":10},"b":{"type":"long"}}}}""")));
        assertFalse(merger.add(new CompressedXContent("""
            {"_doc":{"properties":{"a":{"type":"keyword","ignore_above":20},"c":{"type":"long"}}}}""")));

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(mapperService.documentMapper().mappingSource().string(), equalTo("""
            {"_doc":{"properties":{"a":{"type":"keyword","ignore_above":10},"b":{"type":"long"}}}}"""));
    }

    public void testUpdateWithDifferentObjectParameterIsRejected() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(new CompressedXContent("""
            {"_doc":{"properties":{"obj":{"properties":{"a":{"type":"long"}}}}}}"""));
        assertTrue(merger.add(new CompressedXContent("""
            {"_doc":{"properties":{"obj":{"properties":{"b":{"type":"long"}}}}}}""")));
        assertFalse(merger.add(new CompressedXContent("""
            {"_doc":{"properties":{"obj":{"dynamic":"false","properties":{"c":{"type":"long"}}}}}}""")));

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(mapperService.documentMapper().mappingSource().string(), equalTo("""
            {"_doc":{"properties":{"obj":{"properties":{"a":{"type":"long"},"b":{"type":"long"}}}}}}"""));
    }

    /**
     * A runtime field replaces the one that has the same name when mappings are merged, so an update that defines a runtime
     * field differently is rejected.
     */
    public void testUpdateWithDifferentRuntimeFieldIsRejected() throws IOException {
        MapperService mapperService = createMapperService(topMapping(b -> b.field("dynamic", "runtime")));
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(dynamicUpdate(mapperService, """
            {"a":1}"""));
        assertTrue(merger.add(dynamicUpdate(mapperService, """
            {"a":2,"b":"text"}""")));
        assertFalse(merger.add(dynamicUpdate(mapperService, """
            {"a":"text","c":1}""")));

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(mapperService.fieldType("a").typeName(), equalTo("long"));
        assertThat(mapperService.fieldType("b").typeName(), equalTo("keyword"));
        assertThat(mapperService.fieldType("c"), nullValue());
    }

    /**
     * A runtime field prevents the field of the same path from being mapped dynamically, so an update is rejected when it defines
     * a field where a previous update defines a runtime field, or the other way around.
     */
    public void testFieldAndRuntimeFieldOfSamePathAreRejected() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        CompressedXContent field = new CompressedXContent("""
            {"_doc":{"properties":{"obj":{"properties":{"a":{"type":"long"}}}}}}""");
        CompressedXContent runtimeField = new CompressedXContent("""
            {"_doc":{"runtime":{"obj.a":{"type":"long"}}}}""");
        boolean fieldFirst = randomBoolean();
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(fieldFirst ? field : runtimeField);
        assertFalse(merger.add(fieldFirst ? runtimeField : field));
        assertThat(merger.merged(), sameInstance(fieldFirst ? field : runtimeField));
    }

    /**
     * An update that maps an object where the index has a runtime field changes how the following documents are parsed, so it is
     * not combined with any other update.
     */
    public void testUpdateOverRuntimeFieldOfTheIndexIsRejected() throws IOException {
        MapperService mapperService = createMapperService(runtimeMapping(b -> b.startObject("hidden").field("type", "long").endObject()));
        CompressedXContent object = dynamicUpdate(mapperService, """
            {"hidden":{"sub":1}}""");
        CompressedXContent other = dynamicUpdate(mapperService, """
            {"other":1}""");

        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(object);
        assertFalse(merger.hasCapacity());
        assertFalse(merger.add(other));
        assertThat(merger.merged(), sameInstance(object));

        merger = mapperService.dynamicMappingUpdateMerger(other);
        assertFalse(merger.add(object));
        assertThat(merger.merged(), sameInstance(other));
    }

    /**
     * A field that is an object in one update and a value in another one can't be combined, whichever comes first.
     */
    public void testObjectAndValueAreRejected() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        String value = """
            {"a":1}""";
        String object = """
            {"a":{"b":1}}""";
        boolean valueFirst = randomBoolean();
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(
            dynamicUpdate(mapperService, valueFirst ? value : object)
        );
        assertFalse(merger.add(dynamicUpdate(mapperService, valueFirst ? object : value)));
        assertThat(merger.merged(), equalTo(dynamicUpdate(mapperService, valueFirst ? value : object)));
    }

    /**
     * Updates that only define what the previous ones define are accepted and leave the combined update unchanged.
     */
    public void testUpdatesThatDefineNothingNewAreAccepted() throws IOException {
        MapperService mapperService = createMapperService(mapping(b -> {}));
        CompressedXContent first = dynamicUpdate(mapperService, """
            {"a":1,"obj":{"b":"text"}}""");
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(first);
        for (int i = 0; i < 10; i++) {
            assertTrue(merger.add(dynamicUpdate(mapperService, randomFrom("""
                {"a":2}""", """
                {"obj":{"b":"other"}}""", """
                {"obj":{"b":"other"},"a":3}"""))));
        }
        assertThat(merger.merged(), sameInstance(first));

        assertTrue(merger.add(dynamicUpdate(mapperService, """
            {"obj":{"c":1}}""")));
        assertTrue(merger.add(dynamicUpdate(mapperService, """
            {"obj":{"c":2},"a":4}""")));
        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(mapperService.fieldType("a"), notNullValue());
        assertThat(mapperService.fieldType("obj.b"), notNullValue());
        assertThat(mapperService.fieldType("obj.c"), notNullValue());
    }

    public void testUpdatesBeyondTotalFieldsLimitAreRejected() throws IOException {
        Settings settings = Settings.builder()
            .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), 3)
            .put(MapperService.INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_LIMIT_SETTING.getKey(), randomBoolean())
            .build();
        MapperService mapperService = createMapperService(settings, mapping(b -> b.startObject("f0").field("type", "long").endObject()));
        DynamicMappingUpdateMerger merger = mapperService.dynamicMappingUpdateMerger(dynamicUpdate(mapperService, """
            {"f1":1}"""));
        assertTrue(merger.hasCapacity());
        assertTrue(merger.add(dynamicUpdate(mapperService, """
            {"f2":1}""")));
        // already part of the combined update, doesn't count twice
        assertTrue(merger.add(dynamicUpdate(mapperService, """
            {"f1":2}""")));
        assertFalse(merger.hasCapacity());
        assertFalse(merger.add(dynamicUpdate(mapperService, """
            {"f3":1}""")));

        mapperService.merge(MapperService.SINGLE_MAPPING_NAME, merger.merged(), MergeReason.MAPPING_AUTO_UPDATE);
        assertThat(mapperService.fieldType("f1"), notNullValue());
        assertThat(mapperService.fieldType("f2"), notNullValue());
        assertThat(mapperService.fieldType("f3"), nullValue());
    }

    private static CompressedXContent dynamicUpdate(MapperService mapperService, String doc) {
        return mapperService.documentMapper().parse(source(doc)).dynamicMappingsUpdate();
    }
}
