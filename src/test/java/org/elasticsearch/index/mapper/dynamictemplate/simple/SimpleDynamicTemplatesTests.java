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

package org.elasticsearch.index.mapper.dynamictemplate.simple;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleDynamicTemplatesTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testMatchTypeOnly() throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject().startObject("person").startArray("dynamic_templates").startObject().startObject("test")
                .field("match_mapping_type", "string")
                .startObject("mapping").field("index", "no").endObject()
                .endObject().endObject().endArray().endObject().endObject();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(builder.string());
        builder = JsonXContent.contentBuilder();
        builder.startObject().field("_id", "1").field("s", "hello").field("l", 1).endObject();
        docMapper.parse(builder.bytes());

        DocumentFieldMappers mappers = docMapper.mappers();

        assertThat(mappers.smartName("s"), Matchers.notNullValue());
        assertThat(mappers.smartName("s").mapper().fieldType().indexed(), equalTo(false));

        assertThat(mappers.smartName("l"), Matchers.notNullValue());
        assertThat(mappers.smartName("l").mapper().fieldType().indexed(), equalTo(true));


    }


    @Test
    public void testSimple() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-data.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(false));

        FieldMappers fieldMappers = docMapper.mappers().fullName("name");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi1");
        assertThat(f.name(), equalTo("multi1"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMappers = docMapper.mappers().fullName("multi1");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi1.org");
        assertThat(f.name(), equalTo("multi1.org"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMappers = docMapper.mappers().fullName("multi1.org");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi2");
        assertThat(f.name(), equalTo("multi2"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMappers = docMapper.mappers().fullName("multi2");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi2.org");
        assertThat(f.name(), equalTo("multi2.org"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMappers = docMapper.mappers().fullName("multi2.org");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
    }

    @Test
    public void testSimpleWithXContentTraverse() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-mapping.json");
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        DocumentMapper docMapper = parser.parse(mapping);
        docMapper.refreshSource();
        docMapper = parser.parse(docMapper.mappingSource().string());

        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/dynamictemplate/simple/test-data.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();

        IndexableField f = doc.getField("name");
        assertThat(f.name(), equalTo("name"));
        assertThat(f.stringValue(), equalTo("some name"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(false));

        FieldMappers fieldMappers = docMapper.mappers().fullName("name");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi1");
        assertThat(f.name(), equalTo("multi1"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMappers = docMapper.mappers().fullName("multi1");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi1.org");
        assertThat(f.name(), equalTo("multi1.org"));
        assertThat(f.stringValue(), equalTo("multi 1"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMappers = docMapper.mappers().fullName("multi1.org");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi2");
        assertThat(f.name(), equalTo("multi2"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(true));

        fieldMappers = docMapper.mappers().fullName("multi2");
        assertThat(fieldMappers.mappers().size(), equalTo(1));

        f = doc.getField("multi2.org");
        assertThat(f.name(), equalTo("multi2.org"));
        assertThat(f.stringValue(), equalTo("multi 2"));
        assertThat(f.fieldType().indexed(), equalTo(true));
        assertThat(f.fieldType().tokenized(), equalTo(false));

        fieldMappers = docMapper.mappers().fullName("multi2.org");
        assertThat(fieldMappers.mappers().size(), equalTo(1));
    }
}
