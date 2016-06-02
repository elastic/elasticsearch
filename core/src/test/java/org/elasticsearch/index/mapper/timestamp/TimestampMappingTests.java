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

package org.elasticsearch.index.mapper.timestamp;

import org.apache.lucene.index.IndexOptions;
import org.elasticsearch.Version;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class TimestampMappingTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testSimpleDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", source).timestamp(1));

        assertThat(doc.rootDoc().getField("_timestamp"), equalTo(null));
    }

    public void testEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", "yes").endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        BytesReference source = XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes();
        ParsedDocument doc = docMapper.parse(SourceToParse.source("test", "type", "1", source).timestamp(1));

        assertThat(doc.rootDoc().getField("_timestamp").fieldType().stored(), equalTo(true));
        assertNotSame(IndexOptions.NONE, doc.rootDoc().getField("_timestamp").fieldType().indexOptions());
        assertThat(doc.rootDoc().getField("_timestamp").tokenStream(docMapper.mappers().indexAnalyzer(), null), notNullValue());
    }

    public void testDefaultValues() throws Exception {
        Version version;
        do {
            version = randomVersion(random());
        } while (version.before(Version.V_2_0_0_beta1));
        for (String mapping : Arrays.asList(
                XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string(),
                XContentFactory.jsonBuilder().startObject().startObject("type").startObject("_timestamp").endObject().endObject().endObject().string())) {
            DocumentMapper docMapper = createIndex("test", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build()).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
            assertThat(docMapper.timestampFieldMapper().enabled(), equalTo(TimestampFieldMapper.Defaults.ENABLED.enabled));
            assertThat(docMapper.timestampFieldMapper().fieldType().stored(), equalTo(version.onOrAfter(Version.V_2_0_0_beta1)));
            assertThat(docMapper.timestampFieldMapper().fieldType().indexOptions(), equalTo(TimestampFieldMapper.Defaults.FIELD_TYPE.indexOptions()));
            assertThat(docMapper.timestampFieldMapper().fieldType().hasDocValues(), equalTo(version.onOrAfter(Version.V_2_0_0_beta1)));
            assertThat(docMapper.timestampFieldMapper().fieldType().dateTimeFormatter().format(), equalTo(TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT));
            assertAcked(client().admin().indices().prepareDelete("test").execute().get());
        }
    }

    public void testThatDisablingDuringMergeIsWorking() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).endObject()
                .endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();
        DocumentMapper enabledMapper = mapperService.merge("type", new CompressedXContent(enabledMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", false).endObject()
                .endObject().endObject().string();
        DocumentMapper disabledMapper = mapperService.merge("type", new CompressedXContent(disabledMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        assertThat(enabledMapper.timestampFieldMapper().enabled(), is(true));
        assertThat(disabledMapper.timestampFieldMapper().enabled(), is(false));
    }

    // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingDefaultToEpochValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", "1970-01-01")
                    .field("format", "YYYY-MM-dd")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));
        MetaData metaData = client().admin().cluster().prepareState().get().getState().getMetaData();

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(mappingMetaData, true, "test");
        assertThat(request.timestamp(), notNullValue());
        assertThat(request.timestamp(), is(MappingMetaData.Timestamp.parseStringTimestamp("1970-01-01", Joda.forPattern("YYYY-MM-dd"))));
    }

    // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingNowDefaultValue() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", "now")
                    .field("format", "YYYY-MM-dd")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(mappingMetaData, true, "test");
        assertThat(request.timestamp(), notNullValue());

        // We should have less than one minute (probably some ms)
        long delay = System.currentTimeMillis() - Long.parseLong(request.timestamp());
        assertThat(delay, lessThanOrEqualTo(60000L));
    }

    // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testPathMissingWithForcedNullDefaultShouldFail() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("path", "timestamp")
                    .field("default", (String) null)
                .endObject()
                .endObject().endObject();
        try {
            createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));
            fail("we should reject the mapping with a TimestampParsingException: default timestamp can not be set to null");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("default timestamp can not be set to null"));
        }
    }

    // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingWithForcedNullDefaultShouldFail() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", (String) null)
                .endObject()
                .endObject().endObject();

        try {
            createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));
            fail("we should reject the mapping with a TimestampParsingException: default timestamp can not be set to null");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("default timestamp can not be set to null"));
        }
    }

    // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampDefaultAndIgnore() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                    .field("default", "1971-12-26")
                    .field("ignore_missing", false)
                .endObject()
                .endObject().endObject();

        try {
            createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));
            fail("we should reject the mapping with a TimestampParsingException: default timestamp can not be set with ignore_missing set to false");
        } catch (TimestampParsingException e) {
            assertThat(e.getDetailedMessage(), containsString("default timestamp can not be set with ignore_missing set to false"));
        }
    }

    // Issue 4718: was throwing a TimestampParsingException: failed to parse timestamp [null]
    public void testTimestampMissingShouldNotFail() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", "yes")
                .endObject()
                .endObject().endObject();
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                    .field("foo", "bar")
                .endObject();

        MetaData metaData = MetaData.builder().build();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping.string()));

        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);

        IndexRequest request = new IndexRequest("test", "type", "1").source(doc);
        request.process(mappingMetaData, true, "test");

        assertThat(request.timestamp(), notNullValue());

        // We should have less than one minute (probably some ms)
        long delay = System.currentTimeMillis() - Long.parseLong(request.timestamp());
        assertThat(delay, lessThanOrEqualTo(60000L));
    }

    public void testDefaultTimestampStream() throws IOException {
        // Testing null value for default timestamp
        {
            MappingMetaData.Timestamp timestamp = new MappingMetaData.Timestamp(true,
                    TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT, null, null);
            MappingMetaData expected = new MappingMetaData("type", new CompressedXContent("{}".getBytes(StandardCharsets.UTF_8)),
                    new MappingMetaData.Routing(false), timestamp, false);

            BytesStreamOutput out = new BytesStreamOutput();
            expected.writeTo(out);
            out.close();
            BytesReference bytes = out.bytes();

            MappingMetaData metaData = MappingMetaData.PROTO.readFrom(StreamInput.wrap(bytes));

            assertThat(metaData, is(expected));
        }

        // Testing "now" value for default timestamp
        {
            MappingMetaData.Timestamp timestamp = new MappingMetaData.Timestamp(true,
                    TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT, "now", null);
            MappingMetaData expected = new MappingMetaData("type", new CompressedXContent("{}".getBytes(StandardCharsets.UTF_8)),
                    new MappingMetaData.Routing(false), timestamp, false);

            BytesStreamOutput out = new BytesStreamOutput();
            expected.writeTo(out);
            out.close();
            BytesReference bytes = out.bytes();

            MappingMetaData metaData = MappingMetaData.PROTO.readFrom(StreamInput.wrap(bytes));

            assertThat(metaData, is(expected));
        }

        // Testing "ignore_missing" value for default timestamp
        {
            MappingMetaData.Timestamp timestamp = new MappingMetaData.Timestamp(true,
                    TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT, "now", false);
            MappingMetaData expected = new MappingMetaData("type", new CompressedXContent("{}".getBytes(StandardCharsets.UTF_8)),
                    new MappingMetaData.Routing(false), timestamp, false);

            BytesStreamOutput out = new BytesStreamOutput();
            expected.writeTo(out);
            out.close();
            BytesReference bytes = out.bytes();

            MappingMetaData metaData = MappingMetaData.PROTO.readFrom(StreamInput.wrap(bytes));

            assertThat(metaData, is(expected));
        }
    }

    public void testParsingNotDefaultTwiceDoesNotChangeMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp")
                    .field("enabled", true)
                    .field("default", "1970-01-01")
                .endObject().endObject().endObject().string();
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();

        DocumentMapper docMapper = parser.parse("type", new CompressedXContent(mapping));
        docMapper = parser.parse("type", docMapper.mappingSource());
        assertThat(docMapper.mappingSource().string(), equalTo(mapping));
    }

    /**
     * Test for issue #9223
     */
    public void testInitMappers() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("type")
                    .startObject("_timestamp")
                        .field("enabled", true)
                        .field("default", (String) null)
                    .endObject()
                .endObject().endObject().string();
        // This was causing a NPE
        new MappingMetaData(new CompressedXContent(mapping));
    }

    void assertConflict(MapperService mapperService, String type, String mapping1, String mapping2, String conflict) throws IOException {
        mapperService.merge("type", new CompressedXContent(mapping1), MapperService.MergeReason.MAPPING_UPDATE, false);
        try {
            mapperService.merge("type", new CompressedXContent(mapping2), MapperService.MergeReason.MAPPING_UPDATE, false);
            assertNull(conflict);
        } catch (IllegalArgumentException e) {
            assertNotNull(conflict);
            assertThat(e.getMessage(), containsString(conflict));
        }
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_timestamp").field("enabled", true).field("default", "1970").field("format", "YYYY").endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject().field("_timestamp", 2000000).endObject().bytes());
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Field [_timestamp] is a metadata field and cannot be added inside a document"));
        }
    }

    public void testThatEpochCanBeIgnoredWithCustomFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).field("format", "yyyyMMddHH").endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        MetaData metaData = client().admin().cluster().prepareState().get().getState().getMetaData();

        XContentBuilder doc = XContentFactory.jsonBuilder().startObject().endObject();
        IndexRequest request = new IndexRequest("test", "type", "1").source(doc).timestamp("2015060210");
        MappingMetaData mappingMetaData = new MappingMetaData(docMapper);
        request.process(mappingMetaData, true, "test");

        assertThat(request.timestamp(), is("1433239200000"));
    }

    public void testThatIndicesAfter2_0DontSupportUnixTimestampsInAnyDateFormat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_timestamp").field("enabled", true).field("format", "dateOptionalTime").endObject()
                .endObject().endObject().string();
        BytesReference source = XContentFactory.jsonBuilder().startObject().field("field", "value").endObject().bytes();
        // test with 2.x
        DocumentMapper currentMapper = createIndex("new-index").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        // this works with 2.x
        IndexRequest request = new IndexRequest("new-index", "type", "1").source(source).timestamp("1970-01-01");
        request.process(new MappingMetaData(currentMapper), true, "new-index");

        // this fails with 2.x
        request = new IndexRequest("new-index", "type", "1").source(source).timestamp("1234567890");
        try {
            request.process(new MappingMetaData(currentMapper), true, "new-index");
        } catch (Exception e) {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), containsString("failed to parse timestamp [1234567890]"));
        }
    }
}
