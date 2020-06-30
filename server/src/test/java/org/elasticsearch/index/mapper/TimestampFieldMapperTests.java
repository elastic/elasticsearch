/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class TimestampFieldMapperTests extends ESSingleNodeTestCase {

    public void testPostParse() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_timestamp").field("path", "@timestamp").endObject()
            .startObject("properties").startObject("@timestamp").field("type",
                randomBoolean() ? "date" : "date_nanos").endObject().endObject()
            .endObject().endObject());
        DocumentMapper docMapper = createIndex("test").mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        ParsedDocument doc = docMapper.parse(new SourceToParse("test", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp", "2020-12-12")
                .endObject()),
            XContentType.JSON));
        assertThat(doc.rootDoc().getFields("@timestamp").length, equalTo(2));

        Exception e = expectThrows(MapperException.class, () -> docMapper.parse(new SourceToParse("test", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp1", "2020-12-12")
                .endObject()),
            XContentType.JSON)));
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] is missing"));

        e = expectThrows(MapperException.class, () -> docMapper.parse(new SourceToParse("test", "1", BytesReference
            .bytes(XContentFactory.jsonBuilder()
                .startObject()
                .array("@timestamp", "2020-12-12", "2020-12-13")
                .endObject()),
            XContentType.JSON)));
        assertThat(e.getCause().getMessage(), equalTo("data stream timestamp field [@timestamp] encountered multiple values"));
    }

    public void testValidateNonExistingField() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_timestamp").field("path", "non-existing-field").endObject()
            .startObject("properties").startObject("@timestamp").field("type", "date").endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class, () -> createIndex("test").mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), equalTo("timestamp meta field's field_name [non-existing-field] points to a non existing field"));
    }

    public void testValidateInvalidFieldType() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_timestamp").field("path", "@timestamp").endObject()
            .startObject("properties").startObject("@timestamp").field("type", "keyword").endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class, () -> createIndex("test").mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(),
            equalTo("timestamp meta field's field_name [@timestamp] is of type [keyword], but [date,date_nanos] is expected"));
    }

    public void testValidateNotIndexed() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_timestamp").field("path", "@timestamp").endObject()
            .startObject("properties").startObject("@timestamp").field("type", "date").field("index", "false").endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class, () -> createIndex("test").mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), equalTo("timestamp meta field's field_name [@timestamp] is not indexed"));
    }

    public void testValidateNotDocValues() throws IOException {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_timestamp").field("path", "@timestamp").endObject()
            .startObject("properties").startObject("@timestamp").field("type", "date").field("doc_values", "false").endObject().endObject()
            .endObject().endObject());

        Exception e = expectThrows(IllegalArgumentException.class, () -> createIndex("test").mapperService()
            .merge("type", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE));
        assertThat(e.getMessage(), equalTo("timestamp meta field's field_name [@timestamp] doesn't have doc values"));
    }

}
