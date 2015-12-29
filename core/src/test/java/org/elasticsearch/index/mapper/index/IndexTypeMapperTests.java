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

package org.elasticsearch.index.mapper.index;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class IndexTypeMapperTests extends ESSingleNodeTestCase {
    private Settings bwcSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();

    public void testSimpleIndexMapperEnabledBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_index").field("enabled", true).endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test", bwcSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        IndexFieldMapper indexMapper = docMapper.indexMapper();
        assertThat(indexMapper.enabled(), equalTo(true));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("_index"), equalTo("test"));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testExplicitDisabledIndexMapperBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_index").field("enabled", false).endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test", bwcSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        IndexFieldMapper indexMapper = docMapper.metadataMapper(IndexFieldMapper.class);
        assertThat(indexMapper.enabled(), equalTo(false));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("_index"), nullValue());
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testDefaultDisabledIndexMapper() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        IndexFieldMapper indexMapper = docMapper.metadataMapper(IndexFieldMapper.class);
        assertThat(indexMapper.enabled(), equalTo(false));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field", "value")
                .endObject()
                .bytes());

        assertThat(doc.rootDoc().get("_index"), nullValue());
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }

    public void testThatMergingFieldMappingAllowsDisablingBackcompat() throws Exception {
        String mappingWithIndexEnabled = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_index").field("enabled", true).endObject()
                .endObject().endObject().string();
        MapperService mapperService = createIndex("test", bwcSettings).mapperService();
        DocumentMapper mapperEnabled = mapperService.merge("type", new CompressedXContent(mappingWithIndexEnabled), true, false);
        assertThat(mapperEnabled.IndexFieldMapper().enabled(), is(true));

        String mappingWithIndexDisabled = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("_index").field("enabled", false).endObject()
                .endObject().endObject().string();
        DocumentMapper merged = mapperService.merge("type", new CompressedXContent(mappingWithIndexDisabled), false, false);

        assertThat(merged.IndexFieldMapper().enabled(), is(false));
    }

    public void testCustomSettingsBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_index")
                .field("enabled", true)
                .field("store", "yes").endObject()
            .endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test", bwcSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        IndexFieldMapper indexMapper = docMapper.metadataMapper(IndexFieldMapper.class);
        assertThat(indexMapper.enabled(), equalTo(true));
        assertThat(indexMapper.fieldType().stored(), equalTo(true));

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes());

        assertThat(doc.rootDoc().get("_index"), equalTo("test"));
        assertThat(doc.rootDoc().get("field"), equalTo("value"));
    }
}
