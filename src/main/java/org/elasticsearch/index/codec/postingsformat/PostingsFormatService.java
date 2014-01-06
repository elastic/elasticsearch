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

package org.elasticsearch.index.codec.postingsformat;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Map;

/**
 * The {@link PostingsFormatService} provides access to
 * all configured {@link PostingsFormatProvider} instances by
 * {@link PostingsFormatProvider#name() name}.
 *
 * @see CodecService
 */
public class PostingsFormatService extends AbstractIndexComponent {

    private final ImmutableMap<String, PostingsFormatProvider> providers;

    public final static String DEFAULT_FORMAT = "default";

    public PostingsFormatService(Index index) {
        this(index, ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public PostingsFormatService(Index index, @IndexSettings Settings indexSettings) {
        this(index, indexSettings, ImmutableMap.<String, PostingsFormatProvider.Factory>of());
    }

    @Inject
    public PostingsFormatService(Index index, @IndexSettings Settings indexSettings, Map<String, PostingsFormatProvider.Factory> postingFormatFactories) {
        super(index, indexSettings);

        MapBuilder<String, PostingsFormatProvider> providers = MapBuilder.newMapBuilder();

        Map<String, Settings> postingsFormatSettings = indexSettings.getGroups(PostingsFormatProvider.POSTINGS_FORMAT_SETTINGS_PREFIX);
        for (Map.Entry<String, PostingsFormatProvider.Factory> entry : postingFormatFactories.entrySet()) {
            String name = entry.getKey();
            PostingsFormatProvider.Factory factory = entry.getValue();

            Settings settings = postingsFormatSettings.get(name);
            if (settings == null) {
                settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
            }
            providers.put(name, factory.create(name, settings));
        }

        // This is only needed for tests when guice doesn't have the chance to populate the list of PF factories
        for (PreBuiltPostingsFormatProvider.Factory factory : PostingFormats.listFactories()) {
            if (providers.containsKey(factory.name())) {
                continue;
            }
            providers.put(factory.name(), factory.get());
        }

        this.providers = providers.immutableMap();
    }

    public PostingsFormatProvider get(String name) throws ElasticsearchIllegalArgumentException {
        PostingsFormatProvider provider = providers.get(name);
        if (provider == null) {
            throw new ElasticsearchIllegalArgumentException("failed to find postings_format [" + name + "]");
        }
        return provider;
    }
}
