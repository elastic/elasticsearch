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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.nio.charset.StandardCharsets;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleMapperTests extends ESSingleNodeTestCase {
    public void testSimpleMapper() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapper docMapper = new DocumentMapper.Builder(
                new RootObjectMapper.Builder("person")
                        .add(new ObjectMapper.Builder("name").add(new TextFieldMapper.Builder("first").store(true).index(false))),
            indexService.mapperService()).build(indexService.mapperService());

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();

        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().name()), equalTo("shay"));
        doc = docMapper.parse("test", "person", "1", json).rootDoc();
    }

    public void testParseToJsonAndParse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper docMapper = parser.parse("person", new CompressedXContent(mapping));
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse("person", new CompressedXContent(builtMapping));
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = builtDocMapper.parse("test", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().fieldType().name()), equalTo(Uid.createUid("person", "1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().name()), equalTo("shay"));
    }

    public void testSimpleParser() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1.json"));
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().fieldType().name()), equalTo(Uid.createUid("person", "1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().name()), equalTo("shay"));
    }

    public void testSimpleParserNoTypeNoId() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
        BytesReference json = new BytesArray(copyToBytesFromClasspath("/org/elasticsearch/index/mapper/simple/test1-notype-noid.json"));
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();
        assertThat(doc.get(docMapper.uidMapper().fieldType().name()), equalTo(Uid.createUid("person", "1")));
        assertThat(doc.get(docMapper.mappers().getMapper("name.first").fieldType().name()), equalTo("shay"));
    }

    public void testAttributes() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/simple/test-mapping.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper docMapper = parser.parse("person", new CompressedXContent(mapping));

        assertThat((String) docMapper.meta().get("param1"), equalTo("value1"));

        String builtMapping = docMapper.mappingSource().string();
        DocumentMapper builtDocMapper = parser.parse("person", new CompressedXContent(builtMapping));
        assertThat((String) builtDocMapper.meta().get("param1"), equalTo("value1"));
    }

    public void testNoDocumentSent() throws Exception {
        IndexService indexService = createIndex("test");
        DocumentMapper docMapper = new DocumentMapper.Builder(
                new RootObjectMapper.Builder("person")
                        .add(new ObjectMapper.Builder("name").add(new TextFieldMapper.Builder("first").store(true).index(false))),
            indexService.mapperService()).build(indexService.mapperService());

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
            .startObject("foo.bar").field("type", "text").endObject()
            .endObject().endObject().endObject().string();
        try {
            mapperParser.parse("type", new CompressedXContent(mapping));
            fail("Mapping parse should have failed");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot contain '.'"));
        }
    }
}
