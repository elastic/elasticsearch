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

import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.KeywordFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.core.TextFieldMapper;
import org.elasticsearch.index.mapper.core.TokenCountFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.indices.ttl.IndicesTTLService;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Configures classes and services that are shared by indices on each node.
 */
public class IndicesModule extends AbstractModule {

    private final Map<String, Mapper.TypeParser> mapperParsers
        = new LinkedHashMap<>();
    // Use a LinkedHashMap for metadataMappers because iteration order matters
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers
        = new LinkedHashMap<>();
    private final NamedWriteableRegistry namedWritableRegistry;

    public IndicesModule(NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWritableRegistry = namedWriteableRegistry;
        registerBuiltInMappers();
        registerBuiltInMetadataMappers();
        registerBuildInWritables();
    }

    private void registerBuildInWritables() {
        namedWritableRegistry.register(Condition.class, MaxAgeCondition.NAME, MaxAgeCondition::new);
        namedWritableRegistry.register(Condition.class, MaxDocsCondition.NAME, MaxDocsCondition::new);
    }

    private void registerBuiltInMappers() {
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            registerMapper(type.typeName(), new NumberFieldMapper.TypeParser(type));
        }
        registerMapper(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser());
        registerMapper(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser());
        registerMapper(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser());
        registerMapper(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser());
        registerMapper(StringFieldMapper.CONTENT_TYPE, new StringFieldMapper.TypeParser());
        registerMapper(TextFieldMapper.CONTENT_TYPE, new TextFieldMapper.TypeParser());
        registerMapper(KeywordFieldMapper.CONTENT_TYPE, new KeywordFieldMapper.TypeParser());
        registerMapper(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser());
        registerMapper(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        registerMapper(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser());
        registerMapper(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser());
        registerMapper(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());

        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
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
        bindMapperExtension();

        bind(IndicesService.class).asEagerSingleton();
        bind(RecoverySettings.class).asEagerSingleton();
        bind(RecoveryTargetService.class).asEagerSingleton();
        bind(RecoverySource.class).asEagerSingleton();
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(SyncedFlushService.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetaData.class).asEagerSingleton();
        bind(IndicesTTLService.class).asEagerSingleton();
        bind(UpdateHelper.class).asEagerSingleton();
        bind(MetaDataIndexUpgradeService.class).asEagerSingleton();
        bind(NodeServicesProvider.class).asEagerSingleton();
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
}
