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

import org.apache.lucene.util.GeoUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.VersionUtils;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class SimpleExternalMappingTests extends ESSingleNodeTestCase {
    public void testExternalValues() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_1_0_0, Version.CURRENT);
        Settings settings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.documentMapperParser().putRootTypeParser(ExternalMetadataMapper.CONTENT_TYPE,
                new ExternalMetadataMapper.TypeParser());
        mapperService.documentMapperParser().putTypeParser(ExternalMapperPlugin.EXTERNAL,
                new ExternalMapper.TypeParser(ExternalMapperPlugin.EXTERNAL, "foo"));

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
        // norelease update to .before(Version.V_2_2_0 once GeoPointFieldV2 is fully merged
        if (version.onOrBefore(Version.CURRENT)) {
            assertThat(doc.rootDoc().getField("field.point").stringValue(), is("42.0,51.0"));
        } else {
            assertThat(Long.parseLong(doc.rootDoc().getField("field.point").stringValue()), is(GeoUtils.mortonHash(51.0, 42.0)));
        }

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

        assertThat(doc.rootDoc().getField("field.field"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField(ExternalMetadataMapper.FIELD_NAME).stringValue(), is(ExternalMetadataMapper.FIELD_VALUE));

    }

    public void testExternalValuesWithMultifield() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_1_0_0, Version.CURRENT);
        Settings settings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        MapperService mapperService = createIndex("test", settings).mapperService();
        mapperService.documentMapperParser().putTypeParser(ExternalMapperPlugin.EXTERNAL,
                new ExternalMapper.TypeParser(ExternalMapperPlugin.EXTERNAL, "foo"));

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse(
                XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field")
                    .field("type", ExternalMapperPlugin.EXTERNAL)
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
        // norelease update to .before(Version.V_2_2_0 once GeoPointFieldV2 is fully merged
        if (version.onOrBefore(Version.CURRENT)) {
            assertThat(doc.rootDoc().getField("field.point").stringValue(), is("42.0,51.0"));
        } else {
            assertThat(Long.parseLong(doc.rootDoc().getField("field.point").stringValue()), is(GeoUtils.mortonHash(51.0, 42.0)));
        }

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

        assertThat(doc.rootDoc().getField("field.field"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField("field.field.raw"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field.raw").stringValue(), is("foo"));
    }

    public void testExternalValuesWithMultifieldTwoLevels() throws Exception {
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_1_0_0, Version.CURRENT);
        Settings settings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        MapperService mapperService = createIndex("test", settings).mapperService();

        mapperService.documentMapperParser().putTypeParser(ExternalMapperPlugin.EXTERNAL,
                new ExternalMapper.TypeParser(ExternalMapperPlugin.EXTERNAL, "foo"));
        mapperService.documentMapperParser().putTypeParser(ExternalMapperPlugin.EXTERNAL_BIS,
                new ExternalMapper.TypeParser(ExternalMapperPlugin.EXTERNAL_BIS, "bar"));

        DocumentMapper documentMapper = mapperService.documentMapperParser().parse(
                XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field")
                    .field("type", ExternalMapperPlugin.EXTERNAL)
                    .startObject("fields")
                        .startObject("field")
                            .field("type", "string")
                            .startObject("fields")
                                .startObject("generated")
                                    .field("type", ExternalMapperPlugin.EXTERNAL_BIS)
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
        // norelease update to .before(Version.V_2_2_0 once GeoPointFieldV2 is fully merged
        if (version.onOrBefore(Version.CURRENT)) {
            assertThat(doc.rootDoc().getField("field.point").stringValue(), is("42.0,51.0"));
        } else {
            assertThat(Long.parseLong(doc.rootDoc().getField("field.point").stringValue()), is(GeoUtils.mortonHash(51.0, 42.0)));
        }

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
