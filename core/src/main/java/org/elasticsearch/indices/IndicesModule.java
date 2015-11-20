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
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
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
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.memory.IndexingMemoryController;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.indices.ttl.IndicesTTLService;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configures classes and services that are shared by indices on each node.
 */
public class IndicesModule extends AbstractModule {

    private final ExtensionPoint.ClassSet<QueryParser> queryParsers
        = new ExtensionPoint.ClassSet<>("query_parser", QueryParser.class);
    private final ExtensionPoint.InstanceMap<String, Dictionary> hunspellDictionaries
        = new ExtensionPoint.InstanceMap<>("hunspell_dictionary", String.class, Dictionary.class);

    private final Map<String, Mapper.TypeParser> mapperParsers
        = new LinkedHashMap<>();
    // Use a LinkedHashMap for metadataMappers because iteration order matters
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers
        = new LinkedHashMap<>();

    public IndicesModule() {
        registerBuiltinQueryParsers();
        registerBuiltInMappers();
        registerBuiltInMetadataMappers();
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
        registerQueryParser(FilteredQueryParser.class);
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
        registerQueryParser(LimitQueryParser.class);
        registerQueryParser(ScriptQueryParser.class);
        registerQueryParser(GeoDistanceQueryParser.class);
        registerQueryParser(GeoDistanceRangeQueryParser.class);
        registerQueryParser(GeoBoundingBoxQueryParser.class);
        registerQueryParser(GeohashCellQuery.Parser.class);
        registerQueryParser(GeoPolygonQueryParser.class);
        registerQueryParser(QueryFilterParser.class);
        registerQueryParser(FQueryFilterParser.class);
        registerQueryParser(AndQueryParser.class);
        registerQueryParser(OrQueryParser.class);
        registerQueryParser(NotQueryParser.class);
        registerQueryParser(ExistsQueryParser.class);
        registerQueryParser(MissingQueryParser.class);

        if (ShapesAvailability.JTS_AVAILABLE) {
            registerQueryParser(GeoShapeQueryParser.class);
        }
    }

    private void registerBuiltInMappers() {
        registerMapper(ByteFieldMapper.CONTENT_TYPE, new ByteFieldMapper.TypeParser());
        registerMapper(ShortFieldMapper.CONTENT_TYPE, new ShortFieldMapper.TypeParser());
        registerMapper(IntegerFieldMapper.CONTENT_TYPE, new IntegerFieldMapper.TypeParser());
        registerMapper(LongFieldMapper.CONTENT_TYPE, new LongFieldMapper.TypeParser());
        registerMapper(FloatFieldMapper.CONTENT_TYPE, new FloatFieldMapper.TypeParser());
        registerMapper(DoubleFieldMapper.CONTENT_TYPE, new DoubleFieldMapper.TypeParser());
        registerMapper(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser());
        registerMapper(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser());
        registerMapper(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser());
        registerMapper(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser());
        registerMapper(StringFieldMapper.CONTENT_TYPE, new StringFieldMapper.TypeParser());
        registerMapper(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser());
        registerMapper(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        registerMapper(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser());
        registerMapper(TypeParsers.MULTI_FIELD_CONTENT_TYPE, TypeParsers.multiFieldConverterTypeParser);
        registerMapper(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser());
        registerMapper(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());

        if (ShapesAvailability.JTS_AVAILABLE) {
            registerMapper(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
        }
    }

    private void registerBuiltInMetadataMappers() {
        // NOTE: the order is important

        // UID first so it will be the first stored field to load (so will benefit from "fields: []" early termination
        registerMetadataMapper(UidFieldMapper.NAME, new UidFieldMapper.TypeParser());
        registerMetadataMapper(IdFieldMapper.NAME, new IdFieldMapper.TypeParser());
        registerMetadataMapper(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser());
        registerMetadataMapper(IndexFieldMapper.NAME, new IndexFieldMapper.TypeParser());
        registerMetadataMapper(SourceFieldMapper.NAME, new SourceFieldMapper.TypeParser());
        registerMetadataMapper(TypeFieldMapper.NAME, new TypeFieldMapper.TypeParser());
        registerMetadataMapper(AllFieldMapper.NAME, new AllFieldMapper.TypeParser());
        registerMetadataMapper(TimestampFieldMapper.NAME, new TimestampFieldMapper.TypeParser());
        registerMetadataMapper(TTLFieldMapper.NAME, new TTLFieldMapper.TypeParser());
        registerMetadataMapper(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser());
        registerMetadataMapper(ParentFieldMapper.NAME, new ParentFieldMapper.TypeParser());
        // _field_names is not registered here, see #getMapperRegistry: we need to register it
        // last so that it can see all other mappers, including those coming from plugins
    }

    public void registerQueryParser(Class<? extends QueryParser> queryParser) {
        queryParsers.registerExtension(queryParser);
    }

    public void registerHunspellDictionary(String name, Dictionary dictionary) {
        hunspellDictionaries.registerExtension(name, dictionary);
    }

    /**
     * Register a mapper for the given type.
     */
    public synchronized void registerMapper(String type, Mapper.TypeParser parser) {
        if (mapperParsers.containsKey(type)) {
            throw new IllegalArgumentException("A mapper is already registered for type [" + type + "]");
        }
        mapperParsers.put(type, parser);
    }

    /**
     * Register a root mapper under the given name.
     */
    public synchronized void registerMetadataMapper(String name, MetadataFieldMapper.TypeParser parser) {
        if (metadataMapperParsers.containsKey(name)) {
            throw new IllegalArgumentException("A mapper is already registered for metadata mapper [" + name + "]");
        }
        metadataMapperParsers.put(name, parser);
    }

    @Override
    protected void configure() {
        bindQueryParsersExtension();
        bindHunspellExtension();
        bindMapperExtension();

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

    // public for testing
    public synchronized MapperRegistry getMapperRegistry() {
        // NOTE: we register _field_names here so that it has a chance to see all other
        // mappers, including from plugins
        if (metadataMapperParsers.containsKey(FieldNamesFieldMapper.NAME)) {
            throw new IllegalStateException("Metadata mapper [" + FieldNamesFieldMapper.NAME + "] is already registered");
        }
        final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers
            = new LinkedHashMap<>(this.metadataMapperParsers);
        metadataMapperParsers.put(FieldNamesFieldMapper.NAME, new FieldNamesFieldMapper.TypeParser());
        return new MapperRegistry(mapperParsers, metadataMapperParsers);
    }

    protected void bindMapperExtension() {
        bind(MapperRegistry.class).toInstance(getMapperRegistry());
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
