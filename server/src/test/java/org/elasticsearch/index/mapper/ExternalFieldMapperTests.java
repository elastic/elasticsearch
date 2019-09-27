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

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ExternalFieldMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testExternalValues() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        IndexService indexService = createIndex("test", settings);
        MapperRegistry mapperRegistry = new MapperRegistry(
                singletonMap(ExternalMapperPlugin.EXTERNAL, new ExternalMapper.TypeParser(ExternalMapperPlugin.EXTERNAL, "foo")),
                singletonMap(ExternalMetadataMapper.CONTENT_TYPE, new ExternalMetadataMapper.TypeParser()), MapperPlugin.NOOP_FIELD_FILTER);

        Supplier<QueryShardContext> queryShardContext = () -> {
            return indexService.newQueryShardContext(0, null, () -> { throw new UnsupportedOperationException(); }, null);
        };
        DocumentMapperParser parser = new DocumentMapperParser(indexService.getIndexSettings(), indexService.mapperService(),
                indexService.xContentRegistry(), indexService.similarityService(), mapperRegistry, queryShardContext);
        DocumentMapper documentMapper = parser.parse("type", new CompressedXContent(
                Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject(ExternalMetadataMapper.CONTENT_TYPE)
                .endObject()
                .startObject("properties")
                    .startObject("field").field("type", "external").endObject()
                .endObject()
            .endObject().endObject())
        ));

        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());
        GeoPoint point = new GeoPoint().resetFromIndexableField(doc.rootDoc().getField("field.point"));
        assertThat(point.lat(), closeTo(42.0, 1e-5));
        assertThat(point.lon(), closeTo(51.0, 1e-5));

        assertThat(doc.rootDoc().getField("field.shape"), notNullValue());

        assertThat(doc.rootDoc().getField("field.field"), notNullValue());
        assertThat(doc.rootDoc().getField("field.field").stringValue(), is("foo"));

        assertThat(doc.rootDoc().getField(ExternalMetadataMapper.FIELD_NAME).stringValue(), is(ExternalMetadataMapper.FIELD_VALUE));

    }

    public void testExternalValuesWithMultifield() throws Exception {
        IndexService indexService = createIndex("test");
        Map<String, Mapper.TypeParser> mapperParsers = new HashMap<>();
        mapperParsers.put(ExternalMapperPlugin.EXTERNAL, new ExternalMapper.TypeParser(ExternalMapperPlugin.EXTERNAL, "foo"));
        mapperParsers.put(TextFieldMapper.CONTENT_TYPE, new TextFieldMapper.TypeParser());
        mapperParsers.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordFieldMapper.TypeParser());
        MapperRegistry mapperRegistry = new MapperRegistry(mapperParsers, Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);

        Supplier<QueryShardContext> queryShardContext = () -> {
            return indexService.newQueryShardContext(0, null, () -> { throw new UnsupportedOperationException(); }, null);
        };
        DocumentMapperParser parser = new DocumentMapperParser(indexService.getIndexSettings(), indexService.mapperService(),
                indexService.xContentRegistry(), indexService.similarityService(), mapperRegistry, queryShardContext);

        DocumentMapper documentMapper = parser.parse("type", new CompressedXContent(
                Strings
                .toString(XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("field")
                    .field("type", ExternalMapperPlugin.EXTERNAL)
                    .startObject("fields")
                        .startObject("field")
                            .field("type", "text")
                            .field("store", true)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject())));

        ParsedDocument doc = documentMapper.parse(new SourceToParse("test", "1", BytesReference
                .bytes(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("field", "1234")
                        .endObject()),
                XContentType.JSON));

        assertThat(doc.rootDoc().getField("field.bool"), notNullValue());
        assertThat(doc.rootDoc().getField("field.bool").stringValue(), is("T"));

        assertThat(doc.rootDoc().getField("field.point"), notNullValue());
        GeoPoint point = new GeoPoint().resetFromIndexableField(doc.rootDoc().getField("field.point"));
        assertThat(point.lat(), closeTo(42.0, 1E-5));
        assertThat(point.lon(), closeTo(51.0, 1E-5));

        IndexableField shape = doc.rootDoc().getField("field.shape");
        assertThat(shape, notNullValue());

        IndexableField field = doc.rootDoc().getField("field.field");
        assertThat(field, notNullValue());
        assertThat(field.stringValue(), is("foo"));
    }
}
