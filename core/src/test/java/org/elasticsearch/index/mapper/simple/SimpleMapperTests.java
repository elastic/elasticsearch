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

package org.elasticsearch.index.mapper.simple;

import java.nio.charset.StandardCharsets;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleMapperTests extends ESSingleNodeTestCase {

    @Test
    public void testSimpleMapper() throws Exception {
        IndexService indexService = createIndex("test");
        Settings settings = indexService.settingsService().getSettings();
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        DocumentMapper docMapper = doc(settings,
                rootObject("person")
                        .add(object("name").add(stringField("first").store(true).index(false))),
            indexService.mapperService()).build(indexService.mapperService(), mapperParser);

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
        doc = docMapper.parse("test", "person", "1", json).rootDoc();
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testParseToJsonAndParse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper docMapper = parser.parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
//        System.out.println(builtMapping);
        // reparse it
        DocumentMapper builtDocMapper = parser.parse(builtMapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = builtDocMapper.parse("test", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().fieldType().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().fieldType().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1-notype-noid.json"));
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().fieldType().names().indexName()), equalTo(Uid.createUid("person", "1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().names().indexName()), equalTo("shay"));
//        System.out.println("Document: " + doc);
//        System.out.println("Json: " + docMapper.sourceMapper().value(doc));
    }

    @Test
    public void testAttributes() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper docMapper = parser.parse(mapping);

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        String builtMapping = docMapper.mappingSource().string();
        DocumentMapper builtDocMapper = parser.parse(builtMapping);
        assertThat((String) builtDocMapper.meta().get("param1"), equalTo("value1"));
    }

    @Test
    public void testNoDocumentSent() throws Exception {
        IndexService indexService = createIndex("test");
        Settings settings = indexService.settingsService().getSettings();
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        DocumentMapper docMapper = doc(settings,
                rootObject("person")
                        .add(object("name").add(stringField("first").store(true).index(false))),
            indexService.mapperService()).build(indexService.mapperService(), mapperParser);

        BytesReference json = new BytesArray("".getBytes(StandardCharsets.UTF_8));
        try {
            docMapper.parse("test", "person", "1", json).rootDoc();
            fail("this point is never reached");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("failed to parse, document is empty"));
        }
    }

    public void testHazardousFieldNames() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapperParser mapperParser = indexService.mapperService().documentMapperParser();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
            .startObject("foo.bar").field("type", "string").endObject()
            .endObject().endObject().string();
        try {
            mapperParser.parse(mapping);
            fail("Mapping parse should have failed");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot contain '.'"));
        }
    }
}
