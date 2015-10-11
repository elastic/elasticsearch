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

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/** Unit test(s) for IndexService */
public class IndexServiceTests extends ESSingleNodeTestCase {

    @Test
    public void testDetermineShadowEngineShouldBeUsed() {
        Settings regularSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

        Settings shadowSettings = Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .build();

        assertFalse("no shadow replicas for normal settings", IndexService.useShadowEngine(true, regularSettings));
        assertFalse("no shadow replicas for normal settings", IndexService.useShadowEngine(false, regularSettings));
        assertFalse("no shadow replicas for primary shard with shadow settings", IndexService.useShadowEngine(true, shadowSettings));
        assertTrue("shadow replicas for replica shards with shadow settings",IndexService.useShadowEngine(false, shadowSettings));
    }

    public IndexService newIndexService() {
        Settings settings = Settings.builder().put("name", "indexServiceTests").build();
        return createIndex("test", settings);
    }


    public static CompressedXContent filter(QueryBuilder filterBuilder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return new CompressedXContent(builder.string());
    }

    @Test
    public void testFilteringAliases() throws Exception {

        IndexService indexService = newIndexService();
        add(indexService, "cats", filter(termQuery("animal", "cat")));
        add(indexService, "dogs", filter(termQuery("animal", "dog")));
        add(indexService, "all", null);

        assertThat(indexService.getMetaData().getAliases().containsKey("cats"), equalTo(true));
        assertThat(indexService.getMetaData().getAliases().containsKey("dogs"), equalTo(true));
        assertThat(indexService.getMetaData().getAliases().containsKey("turtles"), equalTo(false));

        assertThat(indexService.aliasFilter("cats").toString(), equalTo("animal:cat"));
        assertThat(indexService.aliasFilter("cats", "dogs").toString(), equalTo("animal:cat animal:dog"));

        // Non-filtering alias should turn off all filters because filters are ORed
        assertThat(indexService.aliasFilter("all"), nullValue());
        assertThat(indexService.aliasFilter("cats", "all"), nullValue());
        assertThat(indexService.aliasFilter("all", "cats"), nullValue());

        add(indexService, "cats", filter(termQuery("animal", "feline")));
        add(indexService, "dogs", filter(termQuery("animal", "canine")));
        assertThat(indexService.aliasFilter("dogs", "cats").toString(), equalTo("animal:canine animal:feline"));
    }

    @Test
    public void testAliasFilters() throws Exception {
        IndexService indexService = newIndexService();
        add(indexService, "cats", filter(termQuery("animal", "cat")));
        add(indexService, "dogs", filter(termQuery("animal", "dog")));

        assertThat(indexService.aliasFilter(), nullValue());
        assertThat(indexService.aliasFilter("dogs").toString(), equalTo("animal:dog"));
        assertThat(indexService.aliasFilter("dogs", "cats").toString(), equalTo("animal:dog animal:cat"));

        add(indexService, "cats", filter(termQuery("animal", "feline")));
        add(indexService, "dogs", filter(termQuery("animal", "canine")));

        assertThat(indexService.aliasFilter("dogs", "cats").toString(), equalTo("animal:canine animal:feline"));
    }

    @Test(expected = InvalidAliasNameException.class)
    public void testRemovedAliasFilter() throws Exception {
        IndexService indexService = newIndexService();

        add(indexService, "cats", filter(termQuery("animal", "cat")));
        remove(indexService, "cats");
        indexService.aliasFilter("cats");
    }


    @Test
    public void testUnknownAliasFilter() throws Exception {
        IndexService indexService = newIndexService();
        add(indexService, "cats", filter(termQuery("animal", "cat")));
        add(indexService, "dogs", filter(termQuery("animal", "dog")));

        try {
            indexService.aliasFilter("unknown");
            fail();
        } catch (InvalidAliasNameException e) {
            // all is well
        }
    }

    private void remove(IndexService service, String alias) {
        IndexMetaData build = IndexMetaData.builder(service.getMetaData()).removeAlias(alias).build();
        service.updateMetaData(build);
    }

    private void add(IndexService service, String alias, @Nullable CompressedXContent filter) {
        IndexMetaData build = IndexMetaData.builder(service.getMetaData()).putAlias(AliasMetaData.builder(alias).filter(filter).build()).build();
        service.updateMetaData(build);
    }
}
