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

package org.elasticsearch.index.mapper.boost;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
public class FieldLevelBoostTests extends ESSingleNodeTestCase {

    private static final Settings BW_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0).build();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testBackCompatFieldLevelBoost() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("str_field").field("type", "string").endObject()
                .startObject("int_field").field("type", "integer").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("byte_field").field("type", "byte").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("date_field").field("type", "date").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("double_field").field("type", "double").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("float_field").field("type", "float").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("long_field").field("type", "long").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("short_field").field("type", "short").startObject("norms").field("enabled", true).endObject().endObject()
                .endObject().endObject().endObject()
                .string();

        DocumentMapper docMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
        BytesReference json = XContentFactory.jsonBuilder().startObject()
                .startObject("str_field").field("boost", 2.0).field("value", "some name").endObject()
                .startObject("int_field").field("boost", 3.0).field("value", 10).endObject()
                .startObject("byte_field").field("boost", 4.0).field("value", 20).endObject()
                .startObject("date_field").field("boost", 5.0).field("value", "2012-01-10").endObject()
                .startObject("double_field").field("boost", 6.0).field("value", 30.0).endObject()
                .startObject("float_field").field("boost", 7.0).field("value", 40.0).endObject()
                .startObject("long_field").field("boost", 8.0).field("value", 50).endObject()
                .startObject("short_field").field("boost", 9.0).field("value", 60).endObject()
                .endObject()
                .bytes();
        Document doc = docMapper.parse("test", "person", "1", json).rootDoc();

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

    public void testBackCompatFieldLevelMappingBoost() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
            .startObject("str_field").field("type", "keyword").field("boost", "2.0").endObject()
            .startObject("int_field").field("type", "integer").field("boost", "3.0").endObject()
            .startObject("byte_field").field("type", "byte").field("boost", "4.0").endObject()
            .startObject("date_field").field("type", "date").field("boost", "5.0").endObject()
            .startObject("double_field").field("type", "double").field("boost", "6.0").endObject()
            .startObject("float_field").field("type", "float").field("boost", "7.0").endObject()
            .startObject("long_field").field("type", "long").field("boost", "8.0").endObject()
            .startObject("short_field").field("type", "short").field("boost", "9.0").endObject()
            .endObject().endObject().endObject()
            .string();

        {
            DocumentMapper docMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
            BytesReference json = XContentFactory.jsonBuilder().startObject()
                .field("str_field", "some name")
                .field("int_field", 10)
                .field("byte_field", 20)
                .field("date_field", "2012-01-10")
                .field("double_field", 30.0)
                .field("float_field", 40.0)
                .field("long_field", 50)
                .field("short_field", 60)
                .endObject()
                .bytes();
            Document doc = docMapper.parse("test", "person", "1", json).rootDoc();

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

        {
            DocumentMapper docMapper = createIndex("test2").mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
            BytesReference json = XContentFactory.jsonBuilder().startObject()
                .field("str_field", "some name")
                .field("int_field", 10)
                .field("byte_field", 20)
                .field("date_field", "2012-01-10")
                .field("double_field", 30.0)
                .field("float_field", 40.0)
                .field("long_field", 50)
                .field("short_field", 60)
                .endObject()
                .bytes();
            Document doc = docMapper.parse("test", "person", "1", json).rootDoc();

            IndexableField f = doc.getField("str_field");
            assertThat(f.boost(), equalTo(1f));

            f = doc.getField("int_field");
            assertThat(f.boost(), equalTo(1f));

            f = doc.getField("byte_field");
            assertThat(f.boost(), equalTo(1f));

            f = doc.getField("date_field");
            assertThat(f.boost(), equalTo(1f));

            f = doc.getField("double_field");
            assertThat(f.boost(), equalTo(1f));

            f = doc.getField("float_field");
            assertThat(f.boost(), equalTo(1f));

            f = doc.getField("long_field");
            assertThat(f.boost(), equalTo(1f));

            f = doc.getField("short_field");
            assertThat(f.boost(), equalTo(1f));
        }
    }

    public void testBackCompatInvalidFieldLevelBoost() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("person").startObject("properties")
                .startObject("str_field").field("type", "string").endObject()
                .startObject("int_field").field("type", "integer").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("byte_field").field("type", "byte").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("date_field").field("type", "date").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("double_field").field("type", "double").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("float_field").field("type", "float").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("long_field").field("type", "long").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("short_field").field("type", "short").startObject("norms").field("enabled", true).endObject().endObject()
                .endObject().endObject().endObject()
                .string();

        DocumentMapper docMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("str_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("int_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("byte_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("date_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("double_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("float_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("long_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

        try {
            docMapper.parse("test", "person", "1", XContentFactory.jsonBuilder().startObject()
                    .startObject("short_field").field("foo", "bar")
                    .endObject().endObject().bytes()).rootDoc();
            fail();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(MapperParsingException.class));
        }

    }

}
