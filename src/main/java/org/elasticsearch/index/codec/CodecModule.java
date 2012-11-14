/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.codec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40PostingsFormat;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing40PostingsFormat;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.codec.postingsformat.PreBuiltPostingsFormatProvider;

import java.util.List;
import java.util.Map;

/**
 */
public class CodecModule extends AbstractModule {

    public static final ImmutableList<PreBuiltPostingsFormatProvider.Factory> preConfiguredPostingFormats;

    static {
        List<PreBuiltPostingsFormatProvider.Factory> preConfiguredPostingFormatsX = Lists.newArrayList();
        // add defaults ones
        for (String luceneName : PostingsFormat.availablePostingsFormats()) {
            preConfiguredPostingFormatsX.add(new PreBuiltPostingsFormatProvider.Factory(PostingsFormat.forName(luceneName)));
        }
        preConfiguredPostingFormatsX.add(new PreBuiltPostingsFormatProvider.Factory("direct", new DirectPostingsFormat()));
        preConfiguredPostingFormatsX.add(new PreBuiltPostingsFormatProvider.Factory("memory", new MemoryPostingsFormat()));
        // LUCENE UPGRADE: Need to change this to the relevant ones on a lucene upgrade
        preConfiguredPostingFormatsX.add(new PreBuiltPostingsFormatProvider.Factory("pulsing", new Pulsing40PostingsFormat()));
        preConfiguredPostingFormatsX.add(new PreBuiltPostingsFormatProvider.Factory("bloom_pulsing", new BloomFilteringPostingsFormat(new Pulsing40PostingsFormat())));
        preConfiguredPostingFormatsX.add(new PreBuiltPostingsFormatProvider.Factory("default", new Lucene40PostingsFormat()));
        preConfiguredPostingFormatsX.add(new PreBuiltPostingsFormatProvider.Factory("bloom_default", new BloomFilteringPostingsFormat(new Lucene40PostingsFormat())));

        preConfiguredPostingFormats = ImmutableList.copyOf(preConfiguredPostingFormatsX);
    }

    private final Settings indexSettings;

    private Map<String, Class<? extends PostingsFormatProvider>> customProviders = Maps.newHashMap();

    public CodecModule(Settings indexSettings) {
        this.indexSettings = indexSettings;
    }

    public CodecModule addPostingFormat(String name, Class<? extends PostingsFormatProvider> provider) {
        this.customProviders.put(name, provider);
        return this;
    }

    @Override
    protected void configure() {

        Map<String, Class<? extends PostingsFormatProvider>> postingFormatProviders = Maps.newHashMap(customProviders);

        Map<String, Settings> postingsFormatsSettings = indexSettings.getGroups("index.codec.postings_format");
        for (Map.Entry<String, Settings> entry : postingsFormatsSettings.entrySet()) {
            String name = entry.getKey();
            Settings settings = entry.getValue();

            Class<? extends PostingsFormatProvider> type =
                    settings.getAsClass("type", null, "org.elasticsearch.index.codec.postingsformat.", "PostingsFormatProvider");

            if (type == null) {
                // nothing found, see if its in bindings as a binding name
                throw new ElasticSearchIllegalArgumentException("PostingsFormat Factory [" + name + "] must have a type associated with it");
            }
            postingFormatProviders.put(name, type);
        }

        // now bind
        MapBinder<String, PostingsFormatProvider.Factory> postingFormatFactoryBinder
                = MapBinder.newMapBinder(binder(), String.class, PostingsFormatProvider.Factory.class);

        for (Map.Entry<String, Class<? extends PostingsFormatProvider>> entry : postingFormatProviders.entrySet()) {
            postingFormatFactoryBinder.addBinding(entry.getKey()).toProvider(FactoryProvider.newFactory(PostingsFormatProvider.Factory.class, entry.getValue())).in(Scopes.SINGLETON);
        }

        for (PreBuiltPostingsFormatProvider.Factory factory : preConfiguredPostingFormats) {
            if (postingFormatProviders.containsKey(factory.name())) {
                continue;
            }
            postingFormatFactoryBinder.addBinding(factory.name()).toInstance(factory);
        }

        bind(PostingsFormatService.class).asEagerSingleton();
        bind(CodecService.class).asEagerSingleton();
    }
}
