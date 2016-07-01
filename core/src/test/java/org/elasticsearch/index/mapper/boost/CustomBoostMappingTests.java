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

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CustomBoostMappingTests extends ESSingleNodeTestCase {

    private static final Settings BW_SETTINGS = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_2_0_0).build();

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testBackCompatCustomBoostValues() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                .startObject("s_field").field("type", "string").endObject()
                .startObject("l_field").field("type", "long").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("i_field").field("type", "integer").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("sh_field").field("type", "short").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("b_field").field("type", "byte").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("d_field").field("type", "double").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("f_field").field("type", "float").startObject("norms").field("enabled", true).endObject().endObject()
                .startObject("date_field").field("type", "date").startObject("norms").field("enabled", true).endObject().endObject()
                .endObject().endObject().endObject().string();

        DocumentMapper mapper = createIndex("test", BW_SETTINGS).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .startObject("s_field").field("value", "s_value").field("boost", 2.0f).endObject()
                .startObject("l_field").field("value", 1L).field("boost", 3.0f).endObject()
                .startObject("i_field").field("value", 1).field("boost", 4.0f).endObject()
                .startObject("sh_field").field("value", 1).field("boost", 5.0f).endObject()
                .startObject("b_field").field("value", 1).field("boost", 6.0f).endObject()
                .startObject("d_field").field("value", 1).field("boost", 7.0f).endObject()
                .startObject("f_field").field("value", 1).field("boost", 8.0f).endObject()
                .startObject("date_field").field("value", "20100101").field("boost", 9.0f).endObject()
                .endObject().bytes());

        assertThat(doc.rootDoc().getField("s_field").boost(), equalTo(2.0f));
        assertThat(doc.rootDoc().getField("l_field").boost(), equalTo(3.0f));
        assertThat(doc.rootDoc().getField("i_field").boost(), equalTo(4.0f));
        assertThat(doc.rootDoc().getField("sh_field").boost(), equalTo(5.0f));
        assertThat(doc.rootDoc().getField("b_field").boost(), equalTo(6.0f));
        assertThat(doc.rootDoc().getField("d_field").boost(), equalTo(7.0f));
        assertThat(doc.rootDoc().getField("f_field").boost(), equalTo(8.0f));
        assertThat(doc.rootDoc().getField("date_field").boost(), equalTo(9.0f));
    }

    public void testBackCompatFieldMappingBoostValues() throws Exception {
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("s_field").field("type", "keyword").field("boost", 2.0f).endObject()
                    .startObject("l_field").field("type", "long").field("boost", 3.0f).endObject()
                    .startObject("i_field").field("type", "integer").field("boost", 4.0f).endObject()
                    .startObject("sh_field").field("type", "short").field("boost", 5.0f).endObject()
                    .startObject("b_field").field("type", "byte").field("boost", 6.0f).endObject()
                    .startObject("d_field").field("type", "double").field("boost", 7.0f).endObject()
                    .startObject("f_field").field("type", "float").field("boost", 8.0f).endObject()
                    .startObject("date_field").field("type", "date").field("boost", 9.0f).endObject()
                    .endObject().endObject().endObject().string();
            IndexService indexService = createIndex("test", BW_SETTINGS);
            QueryShardContext context = indexService.newQueryShardContext();
            DocumentMapper mapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
            DocumentFieldMappers fieldMappers = mapper.mappers();
            assertThat(fieldMappers.getMapper("s_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));
            assertThat(fieldMappers.getMapper("l_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));
            assertThat(fieldMappers.getMapper("i_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));
            assertThat(fieldMappers.getMapper("sh_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));
            assertThat(fieldMappers.getMapper("b_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));
            assertThat(fieldMappers.getMapper("d_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));
            assertThat(fieldMappers.getMapper("f_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));
            assertThat(fieldMappers.getMapper("date_field").fieldType().termQuery("0", context), instanceOf(TermQuery.class));

            ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .field("s_field", "s_value")
                .field("l_field", 1L)
                .field("i_field", 1)
                .field("sh_field", 1)
                .field("b_field", 1)
                .field("d_field", 1)
                .field("f_field", 1)
                .field("date_field", "20100101")
                .endObject().bytes());

            assertThat(doc.rootDoc().getField("s_field").boost(), equalTo(2.0f));
            assertThat(doc.rootDoc().getField("s_field").fieldType().omitNorms(), equalTo(false));
            assertThat(doc.rootDoc().getField("l_field").boost(), equalTo(3.0f));
            assertThat(doc.rootDoc().getField("l_field").fieldType().omitNorms(), equalTo(false));
            assertThat(doc.rootDoc().getField("i_field").boost(), equalTo(4.0f));
            assertThat(doc.rootDoc().getField("i_field").fieldType().omitNorms(), equalTo(false));
            assertThat(doc.rootDoc().getField("sh_field").boost(), equalTo(5.0f));
            assertThat(doc.rootDoc().getField("sh_field").fieldType().omitNorms(), equalTo(false));
            assertThat(doc.rootDoc().getField("b_field").boost(), equalTo(6.0f));
            assertThat(doc.rootDoc().getField("b_field").fieldType().omitNorms(), equalTo(false));
            assertThat(doc.rootDoc().getField("d_field").boost(), equalTo(7.0f));
            assertThat(doc.rootDoc().getField("d_field").fieldType().omitNorms(), equalTo(false));
            assertThat(doc.rootDoc().getField("f_field").boost(), equalTo(8.0f));
            assertThat(doc.rootDoc().getField("f_field").fieldType().omitNorms(), equalTo(false));
            assertThat(doc.rootDoc().getField("date_field").boost(), equalTo(9.0f));
            assertThat(doc.rootDoc().getField("date_field").fieldType().omitNorms(), equalTo(false));
        }

        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("s_field").field("type", "keyword").field("boost", 2.0f).endObject()
                    .startObject("l_field").field("type", "long").field("boost", 3.0f).endObject()
                    .startObject("i_field").field("type", "integer").field("boost", 4.0f).endObject()
                    .startObject("sh_field").field("type", "short").field("boost", 5.0f).endObject()
                    .startObject("b_field").field("type", "byte").field("boost", 6.0f).endObject()
                    .startObject("d_field").field("type", "double").field("boost", 7.0f).endObject()
                    .startObject("f_field").field("type", "float").field("boost", 8.0f).endObject()
                    .startObject("date_field").field("type", "date").field("boost", 9.0f).endObject()
                    .endObject().endObject().endObject().string();
            IndexService indexService = createIndex("text");
            QueryShardContext context = indexService.newQueryShardContext();
            DocumentMapper mapper = indexService.mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
            DocumentFieldMappers fieldMappers = mapper.mappers();
            assertThat(fieldMappers.getMapper("s_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));
            assertThat(fieldMappers.getMapper("l_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));
            assertThat(fieldMappers.getMapper("i_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));
            assertThat(fieldMappers.getMapper("sh_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));
            assertThat(fieldMappers.getMapper("b_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));
            assertThat(fieldMappers.getMapper("d_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));
            assertThat(fieldMappers.getMapper("f_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));
            assertThat(fieldMappers.getMapper("date_field").fieldType().termQuery("0", context), instanceOf(BoostQuery.class));

            ParsedDocument doc = mapper.parse("test", "type", "1", XContentFactory.jsonBuilder().startObject()
                .field("s_field", "s_value")
                .field("l_field", 1L)
                .field("i_field", 1)
                .field("sh_field", 1)
                .field("b_field", 1)
                .field("d_field", 1)
                .field("f_field", 1)
                .field("date_field", "20100101")
                .endObject().bytes());

            assertThat(doc.rootDoc().getField("s_field").boost(), equalTo(1f));
            assertThat(doc.rootDoc().getField("s_field").fieldType().omitNorms(), equalTo(true));
            assertThat(doc.rootDoc().getField("l_field").boost(), equalTo(1f));
            assertThat(doc.rootDoc().getField("i_field").boost(), equalTo(1f));
            assertThat(doc.rootDoc().getField("sh_field").boost(), equalTo(1f));
            assertThat(doc.rootDoc().getField("b_field").boost(), equalTo(1f));
            assertThat(doc.rootDoc().getField("d_field").boost(), equalTo(1f));
            assertThat(doc.rootDoc().getField("f_field").boost(), equalTo(1f));
            assertThat(doc.rootDoc().getField("date_field").boost(), equalTo(1f));
        }
    }
}
