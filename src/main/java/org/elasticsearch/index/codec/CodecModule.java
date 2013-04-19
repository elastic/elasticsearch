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

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.postingsformat.PostingFormats;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.codec.postingsformat.PreBuiltPostingsFormatProvider;

import java.util.Map;

/**
 * The {@link CodecModule} creates and loads the {@link CodecService} and
 * {@link PostingsFormatService} allowing low level data-structure
 * specialization on a Lucene Segment basis.
 * <p>
 * The codec module is the authoritative source for build-in and custom
 * {@link PostingsFormatProvider}. During module bootstrap it processes the
 * index settings underneath the
 * {@value PostingsFormatProvider#POSTINGS_FORMAT_SETTINGS_PREFIX} and
 * instantiates the corresponding {@link PostingsFormatProvider} instances. To
 * configure a custom provider implementations the class should reside in the
 * <tt>org.elasticsearch.index.codec.postingsformat</tt> package and the
 * classname should be suffixed with <tt>PostingsFormatProvider</tt>. <br>
 * For example to expose the Elastic-Fantastic format provider one need to
 * provide the following configuration settings and classes:
 * <ol>
 * <li>create a {@link PostingsFormatProvider} subclass in the package
 * <tt>org.elasticsearch.index.codec.postingsformat</tt></li>
 * 
 * <li>name the subclass <tt>ElasticFantatsticPostingsFormatProvider</tt></li>
 * 
 * <li>configure the custom format in you index settings under
 * <tt>index.codec.postings_format.elastic_fantatic.type : "ElasticFantatic"</tt>
 * </li>
 * 
 * <li>provide any postings format settings for this custom format under the
 * same key ie.
 * <tt>index.codec.postings_format.elastic_fantatic.performance : "crazy_fast"</tt>
 * </li>
 * </ol>
 * 
 * @see CodecService
 * 
 */
public class CodecModule extends AbstractModule {

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

        Map<String, Settings> postingsFormatsSettings = indexSettings.getGroups(PostingsFormatProvider.POSTINGS_FORMAT_SETTINGS_PREFIX);
        for (Map.Entry<String, Settings> entry : postingsFormatsSettings.entrySet()) {
            String name = entry.getKey();
            Settings settings = entry.getValue();

            String sType = settings.get("type");
            if (sType == null || sType.trim().isEmpty()) {
                throw new ElasticSearchIllegalArgumentException("PostingsFormat Factory [" + name + "] must have a type associated with it");
            }

            Class<? extends PostingsFormatProvider> type;
            try {
                type = settings.getAsClass("type", null, "org.elasticsearch.index.codec.postingsformat.", "PostingsFormatProvider");
            } catch (NoClassSettingsException e) {
                throw new ElasticSearchIllegalArgumentException("The specified type [" + sType + "] for postingsFormat Factory [" + name + "] can't be found");
            }
            postingFormatProviders.put(name, type);
        }

        // now bind
        MapBinder<String, PostingsFormatProvider.Factory> postingFormatFactoryBinder
                = MapBinder.newMapBinder(binder(), String.class, PostingsFormatProvider.Factory.class);

        for (Map.Entry<String, Class<? extends PostingsFormatProvider>> entry : postingFormatProviders.entrySet()) {
            postingFormatFactoryBinder.addBinding(entry.getKey()).toProvider(FactoryProvider.newFactory(PostingsFormatProvider.Factory.class, entry.getValue())).in(Scopes.SINGLETON);
        }

        for (PreBuiltPostingsFormatProvider.Factory factory : PostingFormats.listFactories()) {
            if (postingFormatProviders.containsKey(factory.name())) {
                continue;
            }
            postingFormatFactoryBinder.addBinding(factory.name()).toInstance(factory);
        }

        bind(PostingsFormatService.class).asEagerSingleton();
        bind(CodecService.class).asEagerSingleton();
    }
}
