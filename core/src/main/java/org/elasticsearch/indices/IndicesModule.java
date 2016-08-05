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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
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
import org.elasticsearch.index.mapper.core.ScaledFloatFieldMapper;
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
import org.elasticsearch.plugins.MapperPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures classes and services that are shared by indices on each node.
 */
public class IndicesModule extends AbstractModule {

    private final Map<String, Mapper.TypeParser> mapperParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;
    private final MapperRegistry mapperRegistry;
    private final List<Entry> namedWritables = new ArrayList<>();

    public IndicesModule(List<MapperPlugin> mapperPlugins) {
        this.mapperParsers = getMappers(mapperPlugins);
        this.metadataMapperParsers = getMetadataMappers(mapperPlugins);
        this.mapperRegistry = new MapperRegistry(mapperParsers, metadataMapperParsers);
        registerBuiltinWritables();
    }

    private void registerBuiltinWritables() {
        namedWritables.add(new Entry(Condition.class, MaxAgeCondition.NAME, MaxAgeCondition::new));
        namedWritables.add(new Entry(Condition.class, MaxDocsCondition.NAME, MaxDocsCondition::new));
    }

    public List<Entry> getNamedWriteables() {
        return namedWritables;
    }

    private Map<String, Mapper.TypeParser> getMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();

        // builtin mappers
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            mappers.put(type.typeName(), new NumberFieldMapper.TypeParser(type));
        }
        mappers.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser());
        mappers.put(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser());
        mappers.put(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser());
        mappers.put(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser());
        mappers.put(ScaledFloatFieldMapper.CONTENT_TYPE, new ScaledFloatFieldMapper.TypeParser());
        mappers.put(StringFieldMapper.CONTENT_TYPE, new StringFieldMapper.TypeParser());
        mappers.put(TextFieldMapper.CONTENT_TYPE, new TextFieldMapper.TypeParser());
        mappers.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordFieldMapper.TypeParser());
        mappers.put(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser());
        mappers.put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser());
        mappers.put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            mappers.put(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
        }

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, Mapper.TypeParser> entry : mapperPlugin.getMappers().entrySet()) {
                if (mappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Mapper [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(mappers);
    }

    private Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers(List<MapperPlugin> mapperPlugins) {
        // Use a LinkedHashMap for metadataMappers because iteration order matters
        Map<String, MetadataFieldMapper.TypeParser> metadataMappers = new LinkedHashMap<>();

        // builtin metadata mappers
        // UID first so it will be the first stored field to load (so will benefit from "fields: []" early termination
        metadataMappers.put(UidFieldMapper.NAME, new UidFieldMapper.TypeParser());
        metadataMappers.put(IdFieldMapper.NAME, new IdFieldMapper.TypeParser());
        metadataMappers.put(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser());
        metadataMappers.put(IndexFieldMapper.NAME, new IndexFieldMapper.TypeParser());
        metadataMappers.put(SourceFieldMapper.NAME, new SourceFieldMapper.TypeParser());
        metadataMappers.put(TypeFieldMapper.NAME, new TypeFieldMapper.TypeParser());
        metadataMappers.put(AllFieldMapper.NAME, new AllFieldMapper.TypeParser());
        metadataMappers.put(TimestampFieldMapper.NAME, new TimestampFieldMapper.TypeParser());
        metadataMappers.put(TTLFieldMapper.NAME, new TTLFieldMapper.TypeParser());
        metadataMappers.put(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser());
        metadataMappers.put(ParentFieldMapper.NAME, new ParentFieldMapper.TypeParser());
        // _field_names is not registered here, see below

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : mapperPlugin.getMetadataMappers().entrySet()) {
                if (entry.getKey().equals(FieldNamesFieldMapper.NAME)) {
                    throw new IllegalArgumentException("Plugin cannot contain metadata mapper [" + FieldNamesFieldMapper.NAME + "]");
                }
                if (metadataMappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("MetadataFieldMapper [" + entry.getKey() + "] is already registered");
                }
            }
        }

        // we register _field_names here so that it has a chance to see all other mappers, including from plugins
        metadataMappers.put(FieldNamesFieldMapper.NAME, new FieldNamesFieldMapper.TypeParser());
        return Collections.unmodifiableMap(metadataMappers);
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
    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }

    protected void bindMapperExtension() {
        bind(MapperRegistry.class).toInstance(getMapperRegistry());
    }
}
