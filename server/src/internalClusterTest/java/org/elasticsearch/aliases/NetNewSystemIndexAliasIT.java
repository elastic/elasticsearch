/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.aliases;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;

public class NetNewSystemIndexAliasIT extends ESIntegTestCase {
    public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), NetNewSystemIndexTestPlugin.class);
    }

    public void testGetAliasWithNetNewSystemIndices() throws Exception {
        // make sure the net-new system index has been created
        {
            final IndexRequest request = new IndexRequest(SYSTEM_INDEX_NAME);
            request.source("some_field", "some_value");
            IndexResponse resp = client().index(request).get();
            assertThat(resp.status().getStatus(), is(201));
        }
        ensureGreen();

        GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
        GetAliasesResponse aliasResponse = client().admin().indices().getAliases(getAliasesRequest).get();
        assertThat(aliasResponse.getAliases().size(), is(0));
    }

    public static class NetNewSystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final Settings SETTINGS = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.startObject("_meta");
                    builder.field("version", Version.CURRENT.toString());
                    builder.endObject();

                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject("some_field");
                        builder.field("type", "keyword");
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();

                return Collections.singletonList(
                    SystemIndexDescriptor.builder()
                        .setIndexPattern(SYSTEM_INDEX_NAME + "*")
                        .setPrimaryIndex(SYSTEM_INDEX_NAME)
                        .setDescription("Test system index")
                        .setOrigin(getClass().getName())
                        .setVersionMetaKey("version")
                        .setMappings(builder)
                        .setSettings(SETTINGS)
                        .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                        .setNetNew()
                        .build()
                );
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to build " + SYSTEM_INDEX_NAME + " index mappings", e);
            }
        }

        @Override
        public String getFeatureName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "test plugin";
        }
    }
}
