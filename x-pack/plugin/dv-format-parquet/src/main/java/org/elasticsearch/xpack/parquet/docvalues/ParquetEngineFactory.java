/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.repositories.RepositoriesService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Engine factory for Parquet-backed indices. The shard recovers from an empty store, which leaves a commit without segments and with
 * the history, sequence number and translog data that the engine needs. The factory attaches the stub segments to that commit before it
 * hands the store to a {@link ReadOnlyEngine}, so attach runs inside primary store recovery. Replicas and restarts find segments and
 * attach nothing.
 */
public final class ParquetEngineFactory implements EngineFactory {

    private final Supplier<RepositoriesService> repositories;

    public ParquetEngineFactory(Supplier<RepositoriesService> repositories) {
        this.repositories = repositories;
    }

    @Override
    public ReadOnlyEngine newReadWriteEngine(EngineConfig config) {
        try {
            attachIfEmpty(config);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to attach the Parquet files to shard " + config.getShardId(), e);
        }
        // the engine opens the segments, and each segment keeps the files that it finds in the context
        ParquetObjects objects = ParquetReadContext.repositories(repositories, ProjectId.DEFAULT);
        return ParquetReadContext.with(objects, () -> new ParquetReadOnlyEngine(config));
    }

    private void attachIfEmpty(EngineConfig config) throws IOException {
        Directory directory = config.getStore().directory();
        SegmentInfos last = Lucene.readSegmentInfos(directory);
        if (last.size() > 0) {
            return;
        }
        var settings = config.getIndexSettings().getSettings();
        // the one place that reads the footers: the segment keeps a snapshot of them, so that opening a segment costs no request
        // TODO: the project of the index, once the mount API exists and indices of other projects can be mounted
        String repository = ParquetBackedSettings.REPOSITORY.get(settings);
        List<ParquetBackedSettings.PieceSpec> specs = ParquetBackedSettings.pieceSpecs(settings);
        Map<String, ParquetFileInfo> footers = new LinkedHashMap<>();
        for (ParquetBackedSettings.PieceSpec spec : specs) {
            if (footers.containsKey(spec.path()) == false) {
                ParquetObject file = RepositoryParquetObject.open(repositories.get(), ProjectId.DEFAULT, spec.location(repository));
                footers.put(spec.path(), ParquetFooterReader.read(file));
            }
        }
        ParquetFileInfo first = footers.get(specs.get(0).path());
        List<ColumnBinding> bindings = ParquetBackedSettings.bindings(settings, first);
        List<String> columnPaths = bindings.stream()
            .filter(binding -> binding.isSynthesized() == false)
            .map(ColumnBinding::parquetColumnPath)
            .toList();
        List<ExternalFilePiece> pieces = new ArrayList<>();
        long indexRowBase = 0;
        for (ParquetBackedSettings.PieceSpec spec : specs) {
            ParquetFileInfo footer = footers.get(spec.path());
            first.requireCompatibleWith(footer, columnPaths);
            ExternalFilePiece piece = footer.piece(spec.firstRowGroup(), spec.rowGroupCount(), indexRowBase, columnPaths);
            pieces.add(piece);
            indexRowBase += piece.maxDoc();
        }
        ExternalParquetCodecReader reader = new ExternalParquetCodecReader(bindings, pieces);
        IndexWriterConfig iwc = Lucene.indexWriterConfigWithNoMerging(null)
            .setMergeScheduler(new SerialMergeScheduler())
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND)
            .setCodec(new ParquetAttachCodec())
            .setCommitOnClose(false);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            writer.addIndexes(reader);
            // keep the history, sequence number and translog data of the empty commit that recovery wrote
            writer.setLiveCommitData(last.getUserData().entrySet());
            writer.commit();
        }
    }
}
