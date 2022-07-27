/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class JavaMultiFieldMergeTests extends MapperServiceTestCase {
    public void testMergeMultiField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping1.json");
        MapperService mapperService = createMapperService(mapping);

        assertTrue(mapperService.fieldType("name").isSearchable());
        assertThat(mapperService.fieldType("name.indexed"), nullValue());

        BytesReference json = BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("name", "some name").endObject());
        LuceneDocument doc = mapperService.documentMapper().parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping2.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), nullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());

        doc = mapperService.documentMapper().parse(new SourceToParse("1", json, XContentType.JSON)).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping3.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping4.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), notNullValue());
    }

    public void testUpgradeFromMultiFieldTypeToMultiFields() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping1.json");
        MapperService mapperService = createMapperService(mapping);

        assertTrue(mapperService.fieldType("name").isSearchable());
        assertThat(mapperService.fieldType("name.indexed"), nullValue());

        LuceneDocument doc = mapperService.documentMapper().parse(source(b -> b.field("name", "some name"))).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade1.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), nullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());

        doc = mapperService.documentMapper().parse(source(b -> b.field("name", "some name"))).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade2.json");
        mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        assertTrue(mapperService.fieldType("name").isSearchable());

        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade3.json");
        try {
            mapperService.merge("person", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot update parameter [index] from [true] to [false]"));
            assertThat(e.getMessage(), containsString("Cannot update parameter [store] from [true] to [false]"));
        }

        // There are conflicts, so the `name.not_indexed3` has not been added
        assertTrue(mapperService.fieldType("name").isSearchable());
        assertThat(mapperService.fieldType("name.indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed2"), notNullValue());
        assertThat(mapperService.fieldType("name.not_indexed3"), nullValue());
    }
}
