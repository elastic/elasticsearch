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

package org.elasticsearch.index.mapper.externalvalues;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class SimpleExternalMappingTests extends ESSingleNodeTestCase {

    @Test
    public void testExternalValues() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.documentMapperParser().putRootTypeParser(ExternalMetadataMapper.CONTENT_TYPE,
                new ExternalMetadataMapper.TypeParser());
        mapperService.documentMapperParser().putTypeParser(RegisterExternalTypes.EXTERNAL,
                new ExternalMapper.TypeParser(RegisterExternalTypes.EXTERNAL, "foo"));

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse(
                XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject(ExternalMetadataMapper.CONTENT_TYPE)
                .endObject()
                .startObject("properties")
                    .startObject("field").field("type", "external").endObject()
                .endObject()
            .endObject().endObject().string()
        );

        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());
        assertThat(doc.rootDoc().getField("field.point").stringValue(), is("42.0,51.0"));

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

        assertThat(doc.rootDoc().getField("field.field"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField(ExternalMetadataMapper.FIELD_NAME).stringValue(), is(ExternalMetadataMapper.FIELD_VALUE));

    }

    @Test
    public void testExternalValuesWithMultifield() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();
        mapperService.documentMapperParser().putTypeParser(RegisterExternalTypes.EXTERNAL,
                new ExternalMapper.TypeParser(RegisterExternalTypes.EXTERNAL, "foo"));

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse(
                XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field")
                    .field("type", RegisterExternalTypes.EXTERNAL)
                    .startObject("fields")
                        .startObject("field")
                            .field("type", "string")
                            .field("store", "yes")
                            .startObject("fields")
                                .startObject("raw")
                                    .field("type", "string")
                                    .field("index", "not_analyzed")
                                    .field("store", "yes")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string());

        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());
        assertThat(doc.rootDoc().getField("field.point").stringValue(), is("42.0,51.0"));

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

        assertThat(doc.rootDoc().getField("field.field"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField("field.field.raw"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field.raw").stringValue(), is("foo"));
    }

    @Test
    public void testExternalValuesWithMultifieldTwoLevels() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();

        mapperService.documentMapperParser().putTypeParser(RegisterExternalTypes.EXTERNAL,
                new ExternalMapper.TypeParser(RegisterExternalTypes.EXTERNAL, "foo"));
        mapperService.documentMapperParser().putTypeParser(RegisterExternalTypes.EXTERNAL_BIS,
                new ExternalMapper.TypeParser(RegisterExternalTypes.EXTERNAL_BIS, "bar"));

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse(
                XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field")
                    .field("type", RegisterExternalTypes.EXTERNAL)
                    .startObject("fields")
                        .startObject("field")
                            .field("type", "string")
                            .startObject("fields")
                                .startObject("generated")
                                    .field("type", RegisterExternalTypes.EXTERNAL_BIS)
                                .endObject()
                                .startObject("raw")
                                    .field("type", "string")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("raw")
                            .field("type", "string")
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject()
                .string());

        ParsedDocument doc = documentMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "1234")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());
        assertThat(doc.rootDoc().getField("field.point").stringValue(), is("42.0,51.0"));

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

        assertThat(doc.rootDoc().getField("field.field"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField("field.field.generated.generated"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field.generated.generated").stringValue(), is("bar"));

        assertThat(doc.rootDoc().getField("field.field.raw"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field.raw").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField("field.raw"), notNullValue());
        assertThat(doc.rootDoc().getField("field.raw").stringValue(), is("foo"));
    }
}
