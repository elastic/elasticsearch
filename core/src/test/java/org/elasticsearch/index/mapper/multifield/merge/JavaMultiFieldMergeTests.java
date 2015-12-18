/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.multifield.merge;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class JavaMultiFieldMergeTests extends ESSingleNodeTestCase {
    public void testMergeMultiField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping1.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse(mapping);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), nullValue());

        BytesReference json = XContentFactory.jsonBuilder().startObject().field("name", "some name").endObject().bytes();
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping2.json");
        DocumentMapper docMapper2 = parser.parse(mapping);

        docMapper.merge(docMapper2.mapping(), true, false);

        docMapper.merge(docMapper2.mapping(), false, false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());

        doc = docMapper.parse("test", "person", "1", json).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping3.json");
        DocumentMapper docMapper3 = parser.parse(mapping);

        docMapper.merge(docMapper3.mapping(), true, false);

        docMapper.merge(docMapper3.mapping(), false, false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping4.json");
        DocumentMapper docMapper4 = parser.parse(mapping);

        docMapper.merge(docMapper4.mapping(), true, false);

        docMapper.merge(docMapper4.mapping(), false, false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), notNullValue());
    }

    public void testUpgradeFromMultiFieldTypeToMultiFields() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/test-mapping1.json");
        MapperService mapperService = createIndex("test").mapperService();

        DocumentMapper docMapper = mapperService.merge("person", new CompressedXContent(mapping), true, false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), nullValue());

        BytesReference json = XContentFactory.jsonBuilder().startObject().field("name", "some name").endObject().bytes();
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();
        IndexableField f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, nullValue());


        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade1.json");
        mapperService.merge("person", new CompressedXContent(mapping), false, false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), nullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());

        doc = docMapper.parse("test", "person", "1", json).rootDoc();
        f = doc.getField("name");
        assertThat(f, notNullValue());
        f = doc.getField("name.indexed");
        assertThat(f, notNullValue());

        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade2.json");
        mapperService.merge("person", new CompressedXContent(mapping), false, false);

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());

        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());


        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multifield/merge/upgrade3.json");
        try {
            mapperService.merge("person", new CompressedXContent(mapping), false, false);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("mapper [name] has different [index] values"));
            assertThat(e.getMessage(), containsString("mapper [name] has different [store] values"));
        }

        // There are conflicts, so the `name.not_indexed3` has not been added
        assertNotSame(IndexOptions.NONE, docMapper.mappers().getMapper("name").fieldType().indexOptions());
        assertThat(docMapper.mappers().getMapper("name.indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed2"), notNullValue());
        assertThat(docMapper.mappers().getMapper("name.not_indexed3"), nullValue());
    }
}
