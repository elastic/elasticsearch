/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.boost;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

/**
 */
public class FieldLevelBoostTests extends ElasticsearchTestCase {

    @Test
    public void testFieldLevelBoost() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("str_field").field("type", "string").endObject()
                .startObject("int_field").field("type", "integer").field("omit_norms", false).endObject()
                .startObject("byte_field").field("type", "byte").field("omit_norms", false).endObject()
                .startObject("date_field").field("type", "date").field("omit_norms", false).endObject()
                .startObject("double_field").field("type", "double").field("omit_norms", false).endObject()
                .startObject("float_field").field("type", "float").field("omit_norms", false).endObject()
                .startObject("long_field").field("type", "long").field("omit_norms", false).endObject()
                .startObject("short_field").field("type", "short").field("omit_norms", false).endObject()
                .string();

        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        BytesReference json = XContentFactory.jsonBuilder().startObject().field("_id", "1")
                .startObject("str_field").field("boost", 2.0).field("value", "some name").endObject()
                .startObject("int_field").field("boost", 3.0).field("value", 10).endObject()
                .startObject("byte_field").field("boost", 4.0).field("value", 20).endObject()
                .startObject("date_field").field("boost", 5.0).field("value", "2012-01-10").endObject()
                .startObject("double_field").field("boost", 6.0).field("value", 30.0).endObject()
                .startObject("float_field").field("boost", 7.0).field("value", 40.0).endObject()
                .startObject("long_field").field("boost", 8.0).field("value", 50).endObject()
                .startObject("short_field").field("boost", 9.0).field("value", 60).endObject()
                .bytes();
        Document doc = docMapper.parse(json).rootDoc();

        IndexableField f = doc.getField("str_field");
        assertThat((double) f.boost(), closeTo(2.0, 0.001));

        f = doc.getField("int_field");
        assertThat((double) f.boost(), closeTo(3.0, 0.001));

        f = doc.getField("byte_field");
        assertThat((double) f.boost(), closeTo(4.0, 0.001));

        f = doc.getField("date_field");
        assertThat((double) f.boost(), closeTo(5.0, 0.001));

        f = doc.getField("double_field");
        assertThat((double) f.boost(), closeTo(6.0, 0.001));

        f = doc.getField("float_field");
        assertThat((double) f.boost(), closeTo(7.0, 0.001));

        f = doc.getField("long_field");
        assertThat((double) f.boost(), closeTo(8.0, 0.001));

        f = doc.getField("short_field");
        assertThat((double) f.boost(), closeTo(9.0, 0.001));
    }

    @Test
    public void testInvalidFieldLevelBoost() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("str_field").field("type", "string").endObject()
                .startObject("int_field").field("type", "integer").field("omit_norms", false).endObject()
                .startObject("byte_field").field("type", "byte").field("omit_norms", false).endObject()
                .startObject("date_field").field("type", "date").field("omit_norms", false).endObject()
                .startObject("double_field").field("type", "double").field("omit_norms", false).endObject()
                .startObject("float_field").field("type", "float").field("omit_norms", false).endObject()
                .startObject("long_field").field("type", "long").field("omit_norms", false).endObject()
                .startObject("short_field").field("type", "short").field("omit_norms", false).endObject()
                .string();

        DocumentMapper docMapper = MapperTestUtils.newParser().parse(mapping);
        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("str_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("int_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("byte_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("date_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("double_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("float_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("long_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

        try {
            docMapper.parse(XContentFactory.jsonBuilder().startObject()
                    .field("_id", "1").startObject("short_field").field("foo", "bar")
                    .endObject().bytes()).rootDoc();
            assert false;
        } catch (MapperParsingException ex) {
            // Expected
        }

    }

}
