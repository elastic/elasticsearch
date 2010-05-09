/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.analysis;

import org.elasticsearch.util.inject.Scopes;
import org.elasticsearch.util.inject.assistedinject.FactoryProvider;
import org.elasticsearch.util.inject.multibindings.MapBinder;
import org.elasticsearch.util.settings.Settings;

import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class IcuAnalysisBinderProcessor implements AnalysisModule.AnalysisBinderProcessor {

    @Override public void processTokenFilters(MapBinder<String, TokenFilterFactoryFactory> binder, Map<String, Settings> groupSettings) {
        if (!groupSettings.containsKey("icuNormalizer")) {
            binder.addBinding("icuNormalizer").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, IcuNormalizerTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!groupSettings.containsKey("icu_normalizer")) {
            binder.addBinding("icu_normalizer").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, IcuNormalizerTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }

        if (!groupSettings.containsKey("icuFolding")) {
            binder.addBinding("icuFolding").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, IcuFoldingTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!groupSettings.containsKey("icu_folding")) {
            binder.addBinding("icu_folding").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, IcuFoldingTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }

        if (!groupSettings.containsKey("icuCollation")) {
            binder.addBinding("icuCollation").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, IcuCollationTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
        if (!groupSettings.containsKey("icu_collation")) {
            binder.addBinding("icu_collation").toProvider(FactoryProvider.newFactory(TokenFilterFactoryFactory.class, IcuCollationTokenFilterFactory.class)).in(Scopes.SINGLETON);
        }
    }

    @Override public void processTokenizers(MapBinder<String, TokenizerFactoryFactory> binder, Map<String, Settings> groupSettings) {
    }

    @Override public void processAnalyzers(MapBinder<String, AnalyzerProviderFactory> binder, Map<String, Settings> groupSettings) {
    }
}
