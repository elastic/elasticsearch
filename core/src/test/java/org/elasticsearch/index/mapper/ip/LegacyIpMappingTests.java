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

package org.elasticsearch.index.mapper.ip;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class LegacyIpMappingTests extends ESSingleNodeTestCase {

    private static final Settings BW_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0).build();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testSimpleMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("ip").field("type", "ip").endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("ip", "127.0.0.1")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().getField("ip").numericValue().longValue(), is(2130706433L));
        assertThat(doc.rootDoc().get("ip"), is("2130706433"));
    }

    public void testThatValidIpCanBeConvertedToLong() throws Exception {
        assertThat(LegacyIpFieldMapper.ipToLong("127.0.0.1"), is(2130706433L));
    }

    public void testThatInvalidIpThrowsException() throws Exception {
        try {
            LegacyIpFieldMapper.ipToLong("127.0.011.1111111");
            fail("Expected ip address parsing to fail but did not happen");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not a valid ip address"));
        }
    }

    public void testThatIpv6AddressThrowsException() throws Exception {
        try {
            LegacyIpFieldMapper.ipToLong("2001:db8:0:8d3:0:8a2e:70:7344");
            fail("Expected ip address parsing to fail but did not happen");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("not a valid ipv4 address"));
        }
    }

    public void testIgnoreMalformedOption() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties").startObject("field1")
                .field("type", "ip").field("ignore_malformed", true).endObject().startObject("field2").field("type", "ip")
                .field("ignore_malformed", false).endObject().startObject("field3").field("type", "ip").endObject().endObject().endObject()
                .endObject().string();

        DocumentMapper defaultMapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1",
                XContentFactory.jsonBuilder().startObject().field("field1", "").field("field2", "10.20.30.40").endObject().bytes());
        assertThat(doc.rootDoc().getField("field1"), nullValue());
        assertThat(doc.rootDoc().getField("field2"), notNullValue());

        try {
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject().field("field2", "").endObject().bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        }

        // Verify that the default is false
        try {
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject().field("field3", "").endObject().bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        }

        // Unless the global ignore_malformed option is set to true
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_3_0)
                .put("index.mapping.ignore_malformed", true)
                .build();
        defaultMapper = createIndex("test2", indexSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject().field("field3", "").endObject().bytes());
        assertThat(doc.rootDoc().getField("field3"), nullValue());

        // This should still throw an exception, since field2 is specifically set to ignore_malformed=false
        try {
            defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject().field("field2", "").endObject().bytes());
        } catch (MapperParsingException e) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        }
    }

}
