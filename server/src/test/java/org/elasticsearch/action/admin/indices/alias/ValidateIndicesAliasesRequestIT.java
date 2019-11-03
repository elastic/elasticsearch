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

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

public class ValidateIndicesAliasesRequestIT extends ESSingleNodeTestCase {

    public static class IndicesAliasesPlugin extends Plugin implements ActionPlugin {

        static final Setting<List<String>> ALLOWED_ORIGINS_SETTING = Setting.listSetting(
                "index.aliases.allowed_origins",
                Collections.emptyList(),
                Function.identity(),
                Setting.Property.IndexScope,
                Setting.Property.Dynamic);

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(ALLOWED_ORIGINS_SETTING);
        }

        @Override
        public Collection<RequestValidators.RequestValidator<IndicesAliasesRequest>> indicesAliasesRequestValidators() {
            return Collections.singletonList((request, state, indices) -> {
                for (final Index index : indices) {
                    final List<String> allowedOrigins = ALLOWED_ORIGINS_SETTING.get(state.metaData().index(index).getSettings());
                    if (allowedOrigins.contains(request.origin()) == false) {
                        final String message = String.format(
                                Locale.ROOT,
                                "origin [%s] not allowed for index [%s]",
                                request.origin(),
                                index.getName());
                        return Optional.of(new IllegalStateException(message));
                    }
                }
                return Optional.empty();
            });
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(IndicesAliasesPlugin.class);
    }

    public void testAllowed() {
        final Settings settings = Settings.builder()
                .putList(IndicesAliasesPlugin.ALLOWED_ORIGINS_SETTING.getKey(), Collections.singletonList("allowed"))
                .build();
        createIndex("index", settings);
        final IndicesAliasesRequest request = new IndicesAliasesRequest().origin("allowed");
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index").alias("alias"));
        assertAcked(client().admin().indices().aliases(request).actionGet());
        final GetAliasesResponse response = client().admin().indices().getAliases(new GetAliasesRequest("alias")).actionGet();
        assertThat(response.getAliases().keys().size(), equalTo(1));
        assertThat(response.getAliases().keys().iterator().next().value, equalTo("index"));
        final List<AliasMetaData> aliasMetaData = response.getAliases().get("index");
        assertThat(aliasMetaData, hasSize(1));
        assertThat(aliasMetaData.get(0).alias(), equalTo("alias"));
    }

    public void testNotAllowed() {
        final Settings settings = Settings.builder()
                .putList(IndicesAliasesPlugin.ALLOWED_ORIGINS_SETTING.getKey(), Collections.singletonList("allowed"))
                .build();
        createIndex("index", settings);
        final String origin = randomFrom("", "not-allowed");
        final IndicesAliasesRequest request = new IndicesAliasesRequest().origin(origin);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("index").alias("alias"));
        final Exception e = expectThrows(IllegalStateException.class, () -> client().admin().indices().aliases(request).actionGet());
        assertThat(e, hasToString(containsString("origin [" + origin + "] not allowed for index [index]")));
    }

    public void testSomeAllowed() {
        final Settings fooIndexSettings = Settings.builder()
                .putList(IndicesAliasesPlugin.ALLOWED_ORIGINS_SETTING.getKey(), Collections.singletonList("foo_allowed"))
                .build();
        createIndex("foo", fooIndexSettings);
        final Settings barIndexSettings = Settings.builder()
                .putList(IndicesAliasesPlugin.ALLOWED_ORIGINS_SETTING.getKey(), Collections.singletonList("bar_allowed"))
                .build();
        createIndex("bar", barIndexSettings);
        final String origin = randomFrom("foo_allowed", "bar_allowed");
        final IndicesAliasesRequest request = new IndicesAliasesRequest().origin(origin);
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("foo").alias("alias"));
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index("bar").alias("alias"));
        final Exception e = expectThrows(IllegalStateException.class, () -> client().admin().indices().aliases(request).actionGet());
        final String index = "foo_allowed".equals(origin) ? "bar" : "foo";
        assertThat(e, hasToString(containsString("origin [" + origin + "] not allowed for index [" + index + "]")));
        assertTrue(client().admin().indices().getAliases(new GetAliasesRequest("alias")).actionGet().getAliases().isEmpty());
    }
}
