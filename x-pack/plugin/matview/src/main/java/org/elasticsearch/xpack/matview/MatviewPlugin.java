/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.matview.engine.FoldingEngine;
import org.elasticsearch.xpack.matview.fold.FoldSpec;
import org.elasticsearch.xpack.matview.ingest.MatviewFoldListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Proof of concept for incremental materialized views. The view index stores partial aggregate rows (one per
 * indexed batch and group) and this plugin folds rows that share a group key into one row when segments merge,
 * so the number of stored rows stays bounded by the number of groups instead of growing with the number of writes.
 * <p>
 * The fold is configured through index settings for now; the design calls for it to live in the mapping.
 */
public class MatviewPlugin extends Plugin implements EnginePlugin {

    /**
     * The fields whose values identify a group, as {@code field} or {@code field:type} entries where the type is
     * {@code long} (default) or {@code keyword}; rows with equal keys are folded together on merge.
     */
    public static final Setting<List<String>> FOLD_KEY = Setting.stringListSetting("index.matview.fold.key", Setting.Property.IndexScope);

    /**
     * Fields to reduce on merge, as {@code field:reduction:type} triples, e.g. {@code cpu.sum:sum:double} or
     * {@code cpu.value_count:sum:int}. The type names how the doc values are decoded.
     */
    public static final Setting<List<String>> FOLD_REDUCE = Setting.stringListSetting(
        "index.matview.fold.reduce",
        Setting.Property.IndexScope
    );

    /**
     * How merges fold: {@code sorted} merge-sorts the segments and needs the fold key to be a prefix of the index
     * sort, {@code hash} works on any order, {@code auto} picks sorted when the index sort allows it.
     */
    public static final Setting<String> FOLD_STRATEGY = Setting.simpleString(
        "index.matview.fold.strategy",
        "auto",
        Setting.Property.IndexScope
    );

    /** On a source index: the view index that receives this index's contributions. */
    public static final Setting<String> SOURCE_TARGET = Setting.simpleString("index.matview.target", Setting.Property.IndexScope);

    /** On a source index: the numeric field whose average the view keeps (as sum and count). */
    public static final Setting<String> SOURCE_FIELD = Setting.simpleString("index.matview.source.field", Setting.Property.IndexScope);

    /** On a source index: keyword fields the view groups by in addition to the time bucket. */
    public static final Setting<List<String>> SOURCE_GROUP = Setting.stringListSetting(
        "index.matview.source.group",
        Setting.Property.IndexScope
    );

    /** On a source index: the time bucket of the view. */
    public static final Setting<TimeValue> SOURCE_INTERVAL = Setting.timeSetting(
        "index.matview.source.interval",
        TimeValue.timeValueHours(1),
        Setting.Property.IndexScope
    );

    private static final String TIMESTAMP_FIELD = "@timestamp";

    /** Services the index-time fold needs; set once the node has created its components. */
    private record FoldServices(Client client, ThreadPool threadPool, BlockFactory blockFactory) {}

    private final SetOnce<FoldServices> foldServices = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(FOLD_KEY, FOLD_REDUCE, FOLD_STRATEGY, SOURCE_TARGET, SOURCE_FIELD, SOURCE_GROUP, SOURCE_INTERVAL);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        BigArrays bigArrays = services.bigArrays();
        BlockFactory blockFactory = BlockFactory.builder(bigArrays)
            .breaker(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST))
            .build();
        foldServices.set(new FoldServices(services.client(), services.threadPool(), blockFactory));
        return List.of();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        Settings settings = indexModule.getSettings();
        String target = SOURCE_TARGET.get(settings);
        if (target.isEmpty()) {
            return;
        }
        String field = SOURCE_FIELD.get(settings);
        if (field.isEmpty()) {
            throw new IllegalArgumentException("[" + SOURCE_FIELD.getKey() + "] is required with [" + SOURCE_TARGET.getKey() + "]");
        }
        Rounding.Prepared rounding = Rounding.builder(SOURCE_INTERVAL.get(settings)).build().prepareForUnknown();
        FoldServices services = foldServices.get();
        var listener = new MatviewFoldListener(
            new MatviewFoldListener.Config(target, TIMESTAMP_FIELD, SOURCE_GROUP.get(settings), field, rounding),
            services.client(),
            services.threadPool(),
            services.blockFactory()
        );
        indexModule.addIndexOperationListener(listener);
        indexModule.addIndexEventListener(new IndexEventListener() {
            @Override
            public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
                listener.close();
            }
        });
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (foldSpec(indexSettings) == null) {
            return Optional.empty();
        }
        return Optional.of(FoldingEngine::new);
    }

    /** The fold configured on the index, or {@code null} when the index is not a folding view. */
    @Nullable
    public static FoldSpec foldSpec(IndexSettings indexSettings) {
        List<String> keyEntries = indexSettings.getValue(FOLD_KEY);
        if (keyEntries.isEmpty()) {
            return null;
        }
        List<FoldSpec.KeyField> keys = new ArrayList<>();
        for (String entry : keyEntries) {
            String[] parts = entry.split(":");
            if (parts.length > 2) {
                throw new IllegalArgumentException("[" + FOLD_KEY.getKey() + "] entries must be field or field:type, got [" + entry + "]");
            }
            String type = parts.length == 2 ? parts[1].toLowerCase(Locale.ROOT) : "long";
            keys.add(new FoldSpec.KeyField(parts[0], switch (type) {
                case "long" -> ElementType.LONG;
                case "keyword" -> ElementType.BYTES_REF;
                default -> throw new IllegalArgumentException(
                    "[" + FOLD_KEY.getKey() + "] type must be long or keyword, got [" + type + "]"
                );
            }));
        }
        List<FoldSpec.ReducedField> fields = new ArrayList<>();
        for (String entry : indexSettings.getValue(FOLD_REDUCE)) {
            String[] parts = entry.split(":");
            if (parts.length != 3) {
                throw new IllegalArgumentException(
                    "[" + FOLD_REDUCE.getKey() + "] entries must be field:reduction:type, got [" + entry + "]"
                );
            }
            fields.add(
                new FoldSpec.ReducedField(
                    parts[0],
                    ElementType.valueOf(parts[2].toUpperCase(Locale.ROOT)),
                    FoldSpec.Reduction.valueOf(parts[1].toUpperCase(Locale.ROOT))
                )
            );
        }
        return new FoldSpec(keys, fields);
    }
}
