/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MinAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MinDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MinPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MinPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MinSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.OptimalShardCountCondition;
import org.elasticsearch.action.resync.TransportResyncReplicationAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.BooleanScriptFieldType;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.CompositeRuntimeField;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateScriptFieldType;
import org.elasticsearch.index.mapper.DocCountFieldMapper;
import org.elasticsearch.index.mapper.DoubleScriptFieldType;
import org.elasticsearch.index.mapper.FieldAliasMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoPointScriptFieldType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.IpScriptFieldType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.KeywordScriptFieldType;
import org.elasticsearch.index.mapper.LongScriptFieldType;
import org.elasticsearch.index.mapper.LookupRuntimeFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.PassThroughObjectMapper;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.RuntimeField;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
import org.elasticsearch.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Configures classes and services that are shared by indices on each node.
 */
public class IndicesModule extends AbstractModule {
    private final MapperRegistry mapperRegistry;

    public IndicesModule(List<MapperPlugin> mapperPlugins) {
        this.mapperRegistry = new MapperRegistry(
            getMappers(mapperPlugins),
            getRuntimeFields(mapperPlugins),
            getMetadataMappers(mapperPlugins),
            getFieldFilter(mapperPlugins)
        );
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(Condition.class, MinAgeCondition.NAME, MinAgeCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MinDocsCondition.NAME, MinDocsCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MinSizeCondition.NAME, MinSizeCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MinPrimaryShardSizeCondition.NAME, MinPrimaryShardSizeCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MinPrimaryShardDocsCondition.NAME, MinPrimaryShardDocsCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MaxAgeCondition.NAME, MaxAgeCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MaxDocsCondition.NAME, MaxDocsCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MaxSizeCondition.NAME, MaxSizeCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MaxPrimaryShardSizeCondition.NAME, MaxPrimaryShardSizeCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, MaxPrimaryShardDocsCondition.NAME, MaxPrimaryShardDocsCondition::new),
            new NamedWriteableRegistry.Entry(Condition.class, OptimalShardCountCondition.NAME, OptimalShardCountCondition::new)
        );
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MinAgeCondition.NAME),
                (p, c) -> MinAgeCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MinDocsCondition.NAME),
                (p, c) -> MinDocsCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MinSizeCondition.NAME),
                (p, c) -> MinSizeCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MinPrimaryShardSizeCondition.NAME),
                (p, c) -> MinPrimaryShardSizeCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MinPrimaryShardDocsCondition.NAME),
                (p, c) -> MinPrimaryShardDocsCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxAgeCondition.NAME),
                (p, c) -> MaxAgeCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxDocsCondition.NAME),
                (p, c) -> MaxDocsCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxSizeCondition.NAME),
                (p, c) -> MaxSizeCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxPrimaryShardSizeCondition.NAME),
                (p, c) -> MaxPrimaryShardSizeCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(MaxPrimaryShardDocsCondition.NAME),
                (p, c) -> MaxPrimaryShardDocsCondition.fromXContent(p)
            ),
            new NamedXContentRegistry.Entry(
                Condition.class,
                new ParseField(OptimalShardCountCondition.NAME),
                (p, c) -> OptimalShardCountCondition.fromXContent(p)
            )
        );
    }

    public static Map<String, Mapper.TypeParser> getMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();

        // builtin mappers
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            mappers.put(type.typeName(), type.parser());
        }
        for (RangeType type : RangeType.values()) {
            mappers.put(type.typeName(), type.parser());
        }
        mappers.put(BooleanFieldMapper.CONTENT_TYPE, BooleanFieldMapper.PARSER);
        mappers.put(BinaryFieldMapper.CONTENT_TYPE, BinaryFieldMapper.PARSER);
        mappers.put(CompletionFieldMapper.CONTENT_TYPE, CompletionFieldMapper.PARSER);

        DateFieldMapper.Resolution milliseconds = DateFieldMapper.Resolution.MILLISECONDS;
        mappers.put(milliseconds.type(), DateFieldMapper.MILLIS_PARSER);
        DateFieldMapper.Resolution nanoseconds = DateFieldMapper.Resolution.NANOSECONDS;
        mappers.put(nanoseconds.type(), DateFieldMapper.NANOS_PARSER);

        mappers.put(FieldAliasMapper.CONTENT_TYPE, new FieldAliasMapper.TypeParser());
        mappers.put(FlattenedFieldMapper.CONTENT_TYPE, FlattenedFieldMapper.PARSER);
        mappers.put(GeoPointFieldMapper.CONTENT_TYPE, GeoPointFieldMapper.PARSER);
        mappers.put(IpFieldMapper.CONTENT_TYPE, IpFieldMapper.PARSER);
        mappers.put(KeywordFieldMapper.CONTENT_TYPE, KeywordFieldMapper.PARSER);
        mappers.put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(NestedObjectMapper.CONTENT_TYPE, new NestedObjectMapper.TypeParser());
        mappers.put(PassThroughObjectMapper.CONTENT_TYPE, new PassThroughObjectMapper.TypeParser());
        mappers.put(TextFieldMapper.CONTENT_TYPE, TextFieldMapper.PARSER);

        mappers.put(DenseVectorFieldMapper.CONTENT_TYPE, DenseVectorFieldMapper.PARSER);
        mappers.put(SparseVectorFieldMapper.CONTENT_TYPE, SparseVectorFieldMapper.PARSER);

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, Mapper.TypeParser> entry : mapperPlugin.getMappers().entrySet()) {
                if (mappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Mapper [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(mappers);
    }

    private static Map<String, RuntimeField.Parser> getRuntimeFields(List<MapperPlugin> mapperPlugins) {
        Map<String, RuntimeField.Parser> runtimeParsers = new LinkedHashMap<>();
        runtimeParsers.put(BooleanFieldMapper.CONTENT_TYPE, BooleanScriptFieldType.PARSER);
        runtimeParsers.put(NumberFieldMapper.NumberType.LONG.typeName(), LongScriptFieldType.PARSER);
        runtimeParsers.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), DoubleScriptFieldType.PARSER);
        runtimeParsers.put(IpFieldMapper.CONTENT_TYPE, IpScriptFieldType.PARSER);
        runtimeParsers.put(DateFieldMapper.CONTENT_TYPE, DateScriptFieldType.PARSER);
        runtimeParsers.put(KeywordFieldMapper.CONTENT_TYPE, KeywordScriptFieldType.PARSER);
        runtimeParsers.put(GeoPointFieldMapper.CONTENT_TYPE, GeoPointScriptFieldType.PARSER);
        runtimeParsers.put(CompositeRuntimeField.CONTENT_TYPE, CompositeRuntimeField.PARSER);
        runtimeParsers.put(LookupRuntimeFieldType.CONTENT_TYPE, LookupRuntimeFieldType.PARSER);

        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, RuntimeField.Parser> entry : mapperPlugin.getRuntimeFields().entrySet()) {
                if (runtimeParsers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Runtime field type [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(runtimeParsers);
    }

    private static final Map<String, MetadataFieldMapper.TypeParser> builtInMetadataMappers = initBuiltInMetadataMappers();

    private static final Set<String> builtInMetadataFields = Collections.unmodifiableSet(builtInMetadataMappers.keySet());

    private static Map<String, MetadataFieldMapper.TypeParser> initBuiltInMetadataMappers() {
        Map<String, MetadataFieldMapper.TypeParser> builtInMetadataMappers;
        // Use a LinkedHashMap for metadataMappers because iteration order matters
        builtInMetadataMappers = new LinkedHashMap<>();
        // _ignored first so that we always load it, even if only _id is requested
        builtInMetadataMappers.put(IgnoredFieldMapper.NAME, IgnoredFieldMapper.PARSER);
        // ID second so it will be the first (if no ignored fields) stored field to load
        // (so will benefit from "fields: []" early termination
        builtInMetadataMappers.put(IdFieldMapper.NAME, IdFieldMapper.PARSER);
        builtInMetadataMappers.put(RoutingFieldMapper.NAME, RoutingFieldMapper.PARSER);
        builtInMetadataMappers.put(TimeSeriesIdFieldMapper.NAME, TimeSeriesIdFieldMapper.PARSER);
        builtInMetadataMappers.put(TimeSeriesRoutingHashFieldMapper.NAME, TimeSeriesRoutingHashFieldMapper.PARSER);
        builtInMetadataMappers.put(IndexFieldMapper.NAME, IndexFieldMapper.PARSER);
        builtInMetadataMappers.put(SourceFieldMapper.NAME, SourceFieldMapper.PARSER);
        builtInMetadataMappers.put(NestedPathFieldMapper.NAME, NestedPathFieldMapper.PARSER);
        builtInMetadataMappers.put(VersionFieldMapper.NAME, VersionFieldMapper.PARSER);
        builtInMetadataMappers.put(SeqNoFieldMapper.NAME, SeqNoFieldMapper.PARSER);
        builtInMetadataMappers.put(DocCountFieldMapper.NAME, DocCountFieldMapper.PARSER);
        builtInMetadataMappers.put(DataStreamTimestampFieldMapper.NAME, DataStreamTimestampFieldMapper.PARSER);
        // _field_names must be added last so that it has a chance to see all the other mappers
        builtInMetadataMappers.put(FieldNamesFieldMapper.NAME, FieldNamesFieldMapper.PARSER);
        return Collections.unmodifiableMap(builtInMetadataMappers);
    }

    public static Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, MetadataFieldMapper.TypeParser> metadataMappers = new LinkedHashMap<>();

        int i = 0;
        Map.Entry<String, MetadataFieldMapper.TypeParser> fieldNamesEntry = null;
        for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : builtInMetadataMappers.entrySet()) {
            if (i < builtInMetadataMappers.size() - 1) {
                metadataMappers.put(entry.getKey(), entry.getValue());
            } else {
                assert entry.getKey().equals(FieldNamesFieldMapper.NAME) : "_field_names must be the last registered mapper, order counts";
                fieldNamesEntry = entry;
            }
            i++;
        }
        assert fieldNamesEntry != null;

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

        // we register _field_names here so that it has a chance to see all the other mappers, including from plugins
        metadataMappers.put(fieldNamesEntry.getKey(), fieldNamesEntry.getValue());
        return Collections.unmodifiableMap(metadataMappers);
    }

    /**
     * Returns a set containing all of the builtin metadata fields
     */
    public static Set<String> getBuiltInMetadataFields() {
        return builtInMetadataFields;
    }

    private static Function<String, FieldPredicate> getFieldFilter(List<MapperPlugin> mapperPlugins) {
        Function<String, FieldPredicate> fieldFilter = MapperPlugin.NOOP_FIELD_FILTER;
        for (MapperPlugin mapperPlugin : mapperPlugins) {
            fieldFilter = and(fieldFilter, mapperPlugin.getFieldFilter());
        }
        return fieldFilter;
    }

    private static Function<String, FieldPredicate> and(Function<String, FieldPredicate> first, Function<String, FieldPredicate> second) {
        // the purpose of this method is to not chain no-op field predicates, so that we can easily find out when no plugins plug in
        // a field filter, hence skip the mappings filtering part as a whole, as it requires parsing mappings into a map.
        if (first == MapperPlugin.NOOP_FIELD_FILTER) {
            return second;
        }
        if (second == MapperPlugin.NOOP_FIELD_FILTER) {
            return first;
        }
        return index -> {
            FieldPredicate firstPredicate = first.apply(index);
            FieldPredicate secondPredicate = second.apply(index);
            if (firstPredicate == FieldPredicate.ACCEPT_ALL) {
                return secondPredicate;
            }
            if (secondPredicate == FieldPredicate.ACCEPT_ALL) {
                return firstPredicate;
            }
            return new FieldPredicate.And(firstPredicate, secondPredicate);
        };
    }

    @Override
    protected void configure() {
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(TransportResyncReplicationAction.class).asEagerSingleton();
        bind(PrimaryReplicaSyncer.class).asEagerSingleton();
        bind(RetentionLeaseSyncAction.class).asEagerSingleton();
        bind(RetentionLeaseBackgroundSyncAction.class).asEagerSingleton();
        bind(RetentionLeaseSyncer.class).asEagerSingleton();
    }

    /**
     * A registry for all field mappers.
     */
    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }

}
