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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DynamicTemplatesTests extends ESSingleNodeTestCase {
    public void testMatchTypeOnly() throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject().startObject("_doc").startArray("dynamic_templates").startObject().startObject("test")
                .field("match_mapping_type", "string")
                .startObject("mapping").field("index", false).endObject()
                .endObject().endObject().endArray().endObject().endObject();
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(builder).get();

        MapperService mapperService = index.mapperService();
        DocumentMapper docMapper = mapperService.documentMapper();
        builder = JsonXContent.contentBuilder();
        builder.startObject().field("s", "hello").field("l", 1).endObject();
        ParsedDocument parsedDoc = docMapper.parse(new SourceToParse("test", "1", BytesReference.bytes(builder),
                XContentType.JSON));
        client().admin().indices().preparePutMapping("test")
            .setSource(parsedDoc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();

        assertThat(mapperService.fieldType("s"), notNullValue());
        assertEquals(IndexOptions.NONE, mapperService.fieldType("s").indexOptions());

        assertThat(mapperService.fieldType("l"), notNullValue());
        assertNotSame(IndexOptions.NONE, mapperService.fieldType("l").indexOptions());
    }

    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-mapping.json");
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        DocumentMapper docMapper = index.mapperService().documentMapper();
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-data.json");
        ParsedDocument parsedDoc = docMapper.parse(new SourceToParse("test", "1", new BytesArray(json),
                XContentType.JSON));
        client().admin().indices().preparePutMapping("test")
            .setSource(parsedDoc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();
        docMapper = index.mapperService().documentMapper();
        Document doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("some name")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        Mapper fieldMapper = docMapper.mappers().getMapper("name");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1");
        assertThat(f.name(), equalTo("multi1"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = docMapper.mappers().getMapper("multi1");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1.org");
        assertThat(f.name(), equalTo("multi1.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 1")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = docMapper.mappers().getMapper("multi1.org");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2");
        assertThat(f.name(), equalTo("multi2"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = docMapper.mappers().getMapper("multi2");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2.org");
        assertThat(f.name(), equalTo("multi2.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 2")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = docMapper.mappers().getMapper("multi2.org");
        assertNotNull(fieldMapper);
    }

    public void testSimpleWithXContentTraverse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-mapping.json");
        IndexService index = createIndex("test");
        client().admin().indices().preparePutMapping("test").setSource(mapping, XContentType.JSON).get();
        DocumentMapper docMapper = index.mapperService().documentMapper();
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-data.json");
        ParsedDocument parsedDoc = docMapper.parse(new SourceToParse("test", "1", new BytesArray(json),
                XContentType.JSON));
        client().admin().indices().preparePutMapping("test")
            .setSource(parsedDoc.dynamicMappingsUpdate().toString(), XContentType.JSON).get();
        docMapper = index.mapperService().documentMapper();
        Document doc = parsedDoc.rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("some name")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        Mapper fieldMapper = docMapper.mappers().getMapper("name");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1");
        assertThat(f.name(), equalTo("multi1"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = docMapper.mappers().getMapper("multi1");
        assertNotNull(fieldMapper);

        f = doc.getField("multi1.org");
        assertThat(f.name(), equalTo("multi1.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 1")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = docMapper.mappers().getMapper("multi1.org");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2");
        assertThat(f.name(), equalTo("multi2"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMapper = docMapper.mappers().getMapper("multi2");
        assertNotNull(fieldMapper);

        f = doc.getField("multi2.org");
        assertThat(f.name(), equalTo("multi2.org"));
        assertThat(f.binaryValue(), equalTo(new BytesRef("multi 2")));
        assertNotSame(IndexOptions.NONE, f.fieldType().indexOptions());
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMapper = docMapper.mappers().getMapper("multi2.org");
        assertNotNull(fieldMapper);
    }
}
