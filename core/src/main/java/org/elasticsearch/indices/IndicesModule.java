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

package org.elasticsearch.indices;

import org.apache.lucene.analysis.hunspell.Dictionary;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryParser;
import org.elasticsearch.index.query.MoreLikeThisQueryParser;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.cache.query.IndicesQueryCache;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.indices.ttl.IndicesTTLService;

/**
 * Configures classes and services that are shared by indices on each node.
 */
public class IndicesModule extends AbstractModule {

    private final Settings settings;

    private final ExtensionPoint.ClassSet<QueryParser> queryParsers
        = new ExtensionPoint.ClassSet<>("query_parser", QueryParser.class);
    private final ExtensionPoint.InstanceMap<String, Dictionary> hunspellDictionaries
        = new ExtensionPoint.InstanceMap<>("hunspell_dictionary", String.class, Dictionary.class);

    public IndicesModule(Settings settings) {
        this.settings = settings;
        registerBuiltinQueryParsers();
    }
    
    private void registerBuiltinQueryParsers() {
        registerQueryParser(MatchQueryParser.class);
        registerQueryParser(MultiMatchQueryParser.class);
        registerQueryParser(NestedQueryParser.class);
        registerQueryParser(HasChildQueryParser.class);
        registerQueryParser(HasParentQueryParser.class);
        registerQueryParser(DisMaxQueryParser.class);
        registerQueryParser(IdsQueryParser.class);
        registerQueryParser(MatchAllQueryParser.class);
        registerQueryParser(QueryStringQueryParser.class);
        registerQueryParser(BoostingQueryParser.class);
        registerQueryParser(BoolQueryParser.class);
        registerQueryParser(TermQueryParser.class);
        registerQueryParser(TermsQueryParser.class);
        registerQueryParser(FuzzyQueryParser.class);
        registerQueryParser(RegexpQueryParser.class);
        registerQueryParser(RangeQueryParser.class);
        registerQueryParser(PrefixQueryParser.class);
        registerQueryParser(WildcardQueryParser.class);
        registerQueryParser(ConstantScoreQueryParser.class);
        registerQueryParser(SpanTermQueryParser.class);
        registerQueryParser(SpanNotQueryParser.class);
        registerQueryParser(SpanWithinQueryParser.class);
        registerQueryParser(SpanContainingQueryParser.class);
        registerQueryParser(FieldMaskingSpanQueryParser.class);
        registerQueryParser(SpanFirstQueryParser.class);
        registerQueryParser(SpanNearQueryParser.class);
        registerQueryParser(SpanOrQueryParser.class);
        registerQueryParser(MoreLikeThisQueryParser.class);
        registerQueryParser(WrapperQueryParser.class);
        registerQueryParser(IndicesQueryParser.class);
        registerQueryParser(CommonTermsQueryParser.class);
        registerQueryParser(SpanMultiTermQueryParser.class);
        registerQueryParser(FunctionScoreQueryParser.class);
        registerQueryParser(SimpleQueryStringParser.class);
        registerQueryParser(TemplateQueryParser.class);
        registerQueryParser(TypeQueryParser.class);
        registerQueryParser(ScriptQueryParser.class);
        registerQueryParser(GeoDistanceQueryParser.class);
        registerQueryParser(GeoDistanceRangeQueryParser.class);
        registerQueryParser(GeoBoundingBoxQueryParser.class);
        registerQueryParser(GeohashCellQuery.Parser.class);
        registerQueryParser(GeoPolygonQueryParser.class);
        registerQueryParser(QueryFilterParser.class);
        registerQueryParser(NotQueryParser.class);
        registerQueryParser(ExistsQueryParser.class);
        registerQueryParser(MissingQueryParser.class);

        if (ShapesAvailability.JTS_AVAILABLE) {
            registerQueryParser(GeoShapeQueryParser.class);
        }
    }

    public void registerQueryParser(Class<? extends QueryParser> queryParser) {
        queryParsers.registerExtension(queryParser);
    }

    public void registerHunspellDictionary(String name, Dictionary dictionary) {
        hunspellDictionaries.registerExtension(name, dictionary);
    }

    @Override
    protected void configure() {
        bindQueryParsersExtension();
        bindHunspellExtension();

        bind(IndicesLifecycle.class).to(InternalIndicesLifecycle.class).asEagerSingleton();
        bind(IndicesService.class).asEagerSingleton();
        bind(RecoverySettings.class).asEagerSingleton();
        bind(RecoveryTarget.class).asEagerSingleton();
        bind(RecoverySource.class).asEagerSingleton();
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(IndexingMemoryController.class).asEagerSingleton();
        bind(SyncedFlushService.class).asEagerSingleton();
        bind(IndicesQueryCache.class).asEagerSingleton();
        bind(IndicesRequestCache.class).asEagerSingleton();
        bind(IndicesFieldDataCache.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetaData.class).asEagerSingleton();
        bind(IndicesTTLService.class).asEagerSingleton();
        bind(IndicesWarmer.class).asEagerSingleton();
        bind(UpdateHelper.class).asEagerSingleton();
        bind(MetaDataIndexUpgradeService.class).asEagerSingleton();
        bind(IndicesFieldDataCacheListener.class).asEagerSingleton();
    }

    protected void bindQueryParsersExtension() {
        queryParsers.bind(binder());
        bind(IndicesQueriesRegistry.class).asEagerSingleton();
    }

    protected void bindHunspellExtension() {
        hunspellDictionaries.bind(binder());
        bind(HunspellService.class).asEagerSingleton();
        bind(IndicesAnalysisService.class).asEagerSingleton();
    }
}
