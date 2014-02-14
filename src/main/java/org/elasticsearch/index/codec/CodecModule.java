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

package org.elasticsearch.index.codec;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.assistedinject.FactoryProvider;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.NoClassSettingsException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormats;
import org.elasticsearch.index.codec.docvaluesformat.PreBuiltDocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingFormats;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.index.codec.postingsformat.PreBuiltPostingsFormatProvider;

import java.util.Map;

/**
 * The {@link CodecModule} creates and loads the {@link CodecService},
 * {@link PostingsFormatService} and {@link DocValuesFormatService},
 * allowing low level data-structure specialization on a Lucene Segment basis.
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

    private final Map<String, Class<? extends PostingsFormatProvider>> customPostingsFormatProviders = Maps.newHashMap();
    private final Map<String, Class<? extends DocValuesFormatProvider>> customDocValuesFormatProviders = Maps.newHashMap();

    public CodecModule(Settings indexSettings) {
        this.indexSettings = indexSettings;
    }

    public CodecModule addPostingFormat(String name, Class<? extends PostingsFormatProvider> provider) {
        this.customPostingsFormatProviders.put(name, provider);
        return this;
    }

    public CodecModule addDocValuesFormat(String name, Class<? extends DocValuesFormatProvider> provider) {
        this.customDocValuesFormatProviders.put(name, provider);
        return this;
    }

    private void configurePostingsFormats() {
        Map<String, Class<? extends PostingsFormatProvider>> postingFormatProviders = Maps.newHashMap(customPostingsFormatProviders);

        Map<String, Settings> postingsFormatsSettings = indexSettings.getGroups(PostingsFormatProvider.POSTINGS_FORMAT_SETTINGS_PREFIX);
        for (Map.Entry<String, Settings> entry : postingsFormatsSettings.entrySet()) {
            String name = entry.getKey();
            Settings settings = entry.getValue();

            String sType = settings.get("type");
            if (sType == null || sType.trim().isEmpty()) {
                throw new ElasticsearchIllegalArgumentException("PostingsFormat Factory [" + name + "] must have a type associated with it");
            }

            Class<? extends PostingsFormatProvider> type;
            try {
                type = settings.getAsClass("type", null, "org.elasticsearch.index.codec.postingsformat.", "PostingsFormatProvider");
            } catch (NoClassSettingsException e) {
                throw new ElasticsearchIllegalArgumentException("The specified type [" + sType + "] for postingsFormat Factory [" + name + "] can't be found");
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
    }

    private void configureDocValuesFormats() {
        Map<String, Class<? extends DocValuesFormatProvider>> docValuesFormatProviders = Maps.newHashMap(customDocValuesFormatProviders);

        Map<String, Settings> docValuesFormatSettings = indexSettings.getGroups(DocValuesFormatProvider.DOC_VALUES_FORMAT_SETTINGS_PREFIX);
        for (Map.Entry<String, Settings> entry : docValuesFormatSettings.entrySet()) {
            final String name = entry.getKey();
            final Settings settings = entry.getValue();

            final String sType = settings.get("type");
            if (sType == null || sType.trim().isEmpty()) {
                throw new ElasticsearchIllegalArgumentException("DocValuesFormat Factory [" + name + "] must have a type associated with it");
            }

            final Class<? extends DocValuesFormatProvider> type;
            try {
                type = settings.getAsClass("type", null, "org.elasticsearch.index.codec.docvaluesformat.", "DocValuesFormatProvider");
            } catch (NoClassSettingsException e) {
                throw new ElasticsearchIllegalArgumentException("The specified type [" + sType + "] for docValuesFormat Factory [" + name + "] can't be found");
            }
            docValuesFormatProviders.put(name, type);
        }

        // now bind
        MapBinder<String, DocValuesFormatProvider.Factory> docValuesFormatFactoryBinder
                = MapBinder.newMapBinder(binder(), String.class, DocValuesFormatProvider.Factory.class);

        for (Map.Entry<String, Class<? extends DocValuesFormatProvider>> entry : docValuesFormatProviders.entrySet()) {
            docValuesFormatFactoryBinder.addBinding(entry.getKey()).toProvider(FactoryProvider.newFactory(DocValuesFormatProvider.Factory.class, entry.getValue())).in(Scopes.SINGLETON);
        }

        for (PreBuiltDocValuesFormatProvider.Factory factory : DocValuesFormats.listFactories()) {
            if (docValuesFormatProviders.containsKey(factory.name())) {
                continue;
            }
            docValuesFormatFactoryBinder.addBinding(factory.name()).toInstance(factory);
        }

        bind(DocValuesFormatService.class).asEagerSingleton();
    }

    @Override
    protected void configure() {
        configurePostingsFormats();
        configureDocValuesFormats();

        bind(CodecService.class).asEagerSingleton();
    }
}
