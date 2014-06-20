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

package org.elasticsearch.index.aliases;

import org.elasticsearch.cache.recycler.CacheRecyclerModule;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.cache.IndexCacheModule;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.engine.IndexEngineModule;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.IndexQueryParserModule;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.indices.fielddata.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.indices.fielddata.breaker.CircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class IndexAliasesServiceTests extends ElasticsearchTestCase {
    public static IndexAliasesService newIndexAliasesService() {

        Settings settings = ImmutableSettings.builder().put("name", "IndexAliasesServiceTests").build();
        return new IndexAliasesService(new Index("test"), settings, newIndexQueryParserService(settings));
    }

    public static IndexQueryParserService newIndexQueryParserService(Settings settings) {
        Injector injector = new ModulesBuilder().add(
                new IndicesQueriesModule(),
                new CacheRecyclerModule(settings),
                new CodecModule(settings),
                new IndexSettingsModule(new Index("test"), settings),
                new IndexNameModule(new Index("test")),
                new IndexQueryParserModule(settings),
                new AnalysisModule(settings),
                new SimilarityModule(settings),
                new ScriptModule(settings),
                new SettingsModule(settings),
                new IndexEngineModule(settings),
                new IndexCacheModule(settings),
                new FunctionScoreModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(ClusterService.class).toProvider(Providers.of((ClusterService) null));
                        bind(CircuitBreakerService.class).to(NoneCircuitBreakerService.class);
                    }
                }
        ).createInjector();
        return injector.getInstance(IndexQueryParserService.class);
    }

    public static CompressedString filter(FilterBuilder filterBuilder) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        filterBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return new CompressedString(builder.string());
    }

    @Test
    public void testFilteringAliases() throws Exception {
        IndexAliasesService indexAliasesService = newIndexAliasesService();
        indexAliasesService.add("cats", filter(termFilter("animal", "cat")));
        indexAliasesService.add("dogs", filter(termFilter("animal", "dog")));
        indexAliasesService.add("all", null);

        assertThat(indexAliasesService.hasAlias("cats"), equalTo(true));
        assertThat(indexAliasesService.hasAlias("dogs"), equalTo(true));
        assertThat(indexAliasesService.hasAlias("turtles"), equalTo(false));

        assertThat(indexAliasesService.aliasFilter("cats").toString(), equalTo("cache(animal:cat)"));
        assertThat(indexAliasesService.aliasFilter("cats", "dogs").toString(), equalTo("BooleanFilter(cache(animal:cat) cache(animal:dog))"));

        // Non-filtering alias should turn off all filters because filters are ORed
        assertThat(indexAliasesService.aliasFilter("all"), nullValue());
        assertThat(indexAliasesService.aliasFilter("cats", "all"), nullValue());
        assertThat(indexAliasesService.aliasFilter("all", "cats"), nullValue());

        indexAliasesService.add("cats", filter(termFilter("animal", "feline")));
        indexAliasesService.add("dogs", filter(termFilter("animal", "canine")));
        assertThat(indexAliasesService.aliasFilter("dogs", "cats").toString(), equalTo("BooleanFilter(cache(animal:canine) cache(animal:feline))"));
    }

    @Test
    public void testAliasFilters() throws Exception {
        IndexAliasesService indexAliasesService = newIndexAliasesService();
        indexAliasesService.add("cats", filter(termFilter("animal", "cat")));
        indexAliasesService.add("dogs", filter(termFilter("animal", "dog")));

        assertThat(indexAliasesService.aliasFilter(), nullValue());
        assertThat(indexAliasesService.aliasFilter("dogs").toString(), equalTo("cache(animal:dog)"));
        assertThat(indexAliasesService.aliasFilter("dogs", "cats").toString(), equalTo("BooleanFilter(cache(animal:dog) cache(animal:cat))"));

        indexAliasesService.add("cats", filter(termFilter("animal", "feline")));
        indexAliasesService.add("dogs", filter(termFilter("animal", "canine")));

        assertThat(indexAliasesService.aliasFilter("dogs", "cats").toString(), equalTo("BooleanFilter(cache(animal:canine) cache(animal:feline))"));
    }

    @Test(expected = InvalidAliasNameException.class)
    public void testRemovedAliasFilter() throws Exception {
        IndexAliasesService indexAliasesService = newIndexAliasesService();
        indexAliasesService.add("cats", filter(termFilter("animal", "cat")));
        indexAliasesService.remove("cats");
        indexAliasesService.aliasFilter("cats");
    }


    @Test
    public void testUnknownAliasFilter() throws Exception {
        IndexAliasesService indexAliasesService = newIndexAliasesService();
        indexAliasesService.add("cats", filter(termFilter("animal", "cat")));
        indexAliasesService.add("dogs", filter(termFilter("animal", "dog")));

        try {
            indexAliasesService.aliasFilter("unknown");
            fail();
        } catch (InvalidAliasNameException e) {
            // all is well
        }
    }


}
