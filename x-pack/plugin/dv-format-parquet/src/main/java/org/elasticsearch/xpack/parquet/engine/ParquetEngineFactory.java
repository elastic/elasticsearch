/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.engine;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.parquet.dv.ColumnBinding;
import org.elasticsearch.xpack.parquet.dv.Conversion;
import org.elasticsearch.xpack.parquet.dv.ExternalFilePiece;
import org.elasticsearch.xpack.parquet.dv.ParquetFileInfo;
import org.elasticsearch.xpack.parquet.dv.ParquetReadContext;
import org.elasticsearch.xpack.parquet.repository.CachedStorageObject;
import org.elasticsearch.xpack.parquet.repository.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.repository.ParquetBlobCache;
import org.elasticsearch.xpack.parquet.repository.RepositoryLocation;
import org.elasticsearch.xpack.parquet.repository.RepositoryStorageObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Engine factory for Parquet-backed indices. The shard recovers from an empty store, which leaves a commit without segments and with
 * the history, sequence number and translog data that the engine needs. The factory attaches one stub segment for each piece of the
 * index to that commit before it hands the store to a read-only engine, so attach runs inside primary store recovery. Replicas and
 * restarts find segments and attach nothing.
 */
public final class ParquetEngineFactory implements EngineFactory {

    private final Supplier<RepositoriesService> repositories;
    /** The disk cache of file ranges of the node, which answers {@code null} when the node has none. */
    private final Supplier<ParquetBlobCache> cache;

    public ParquetEngineFactory(Supplier<RepositoriesService> repositories, Supplier<ParquetBlobCache> cache) {
        this.repositories = repositories;
        this.cache = cache;
    }

    /** The file as the reader reads it: through the disk cache of the node when it has one. */
    private StorageObject cached(RepositoryStorageObject object) {
        ParquetBlobCache blobCache = cache.get();
        return blobCache == null ? object : new CachedStorageObject(object, blobCache);
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            attachIfEmpty(config);
            // The engine opens the segments, and each segment keeps the resolver that it finds in the context. A segment stores where
            // its file is and the length it was attached with, so opening it makes no request.
            ParquetReadContext.Resolver resolver = (location, length) -> cached(
                RepositoryStorageObject.open(
                    repositories.get(),
                    // TODO: the project of the index, once the mount API can mount indices of other projects
                    ProjectId.DEFAULT,
                    RepositoryLocation.parse(location),
                    length
                )
            );
            return ParquetReadContext.with(resolver, () -> new ParquetReadOnlyEngine(config));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open the Parquet-backed shard " + config.getShardId(), e);
        }
    }

    private void attachIfEmpty(EngineConfig config) throws IOException {
        Directory directory = config.getStore().directory();
        SegmentInfos last = Lucene.readSegmentInfos(directory);
        if (last.size() > 0) {
            return;
        }
        Settings settings = config.getIndexSettings().getSettings();
        String repository = ParquetBackedSettings.REPOSITORY.get(settings);
        List<ParquetBackedSettings.PieceSpec> specs = ParquetBackedSettings.pieceSpecs(settings);
        // the one place that reads the footers when the shard is created: a segment reads its own footer when it first serves values
        Map<String, ParquetFileInfo> files = new LinkedHashMap<>();
        for (ParquetBackedSettings.PieceSpec spec : specs) {
            if (files.containsKey(spec.path()) == false) {
                RepositoryLocation location = spec.location(repository);
                files.put(
                    spec.path(),
                    ParquetFileInfo.read(
                        location.toString(),
                        cached(RepositoryStorageObject.open(repositories.get(), ProjectId.DEFAULT, location))
                    )
                );
            }
        }
        ParquetFileInfo first = files.get(specs.get(0).path());
        List<ColumnBinding> bindings = bindingsFor(settings, first);
        List<String> columns = bindings.stream()
            .filter(binding -> binding.isSynthesized() == false)
            .map(ColumnBinding::parquetColumn)
            .toList();
        List<ExternalFilePiece> pieces = new ArrayList<>();
        long indexRowBase = 0;
        for (ParquetBackedSettings.PieceSpec spec : specs) {
            ParquetFileInfo file = files.get(spec.path());
            first.requireCompatibleWith(file, columns);
            ExternalFilePiece piece = ExternalFilePiece.of(file, spec.firstRowGroup(), spec.rowGroupCount(), indexRowBase, bindings);
            pieces.add(piece);
            indexRowBase += piece.rowCount();
        }
        IndexWriterConfig iwc = Lucene.indexWriterConfigWithNoMerging(null)
            .setMergeScheduler(new SerialMergeScheduler())
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND)
            .setCodec(new ParquetAttachCodec())
            .setCommitOnClose(false);
        try (IndexWriter writer = new IndexWriter(directory, iwc)) {
            for (ExternalFilePiece piece : pieces) {
                // one stub segment for each piece, which never merges
                writer.addIndexes(new ExternalParquetCodecReader(bindings, piece));
            }
            // keep the history, sequence number and translog data of the empty commit that recovery wrote
            writer.setLiveCommitData(last.getUserData().entrySet());
            writer.commit();
        }
    }

    /**
     * The bindings of the fields of an index to the columns of its first file, and the synthesized {@code _id} when the index is
     * columnar.
     *
     * @throws IllegalArgumentException if a column is missing, nested or repeated, or has a type that the format does not serve yet
     */
    private static List<ColumnBinding> bindingsFor(Settings indexSettings, ParquetFileInfo file) {
        List<ColumnBinding> bindings = new ArrayList<>();
        int number = 0;
        for (ParquetBackedSettings.FieldSpec spec : ParquetBackedSettings.fieldSpecs(indexSettings)) {
            ParquetFileInfo.LeafColumn column = file.column(spec.column());
            if (column == null) {
                throw new IllegalArgumentException("[" + file.location() + "] has no column [" + spec.column() + "]");
            }
            if (column.flat() == false || column.repeated()) {
                throw new IllegalArgumentException("column [" + column.path() + "] is nested or repeated, which is not supported yet");
            }
            DocValuesType type = docValuesType(column);
            if (spec.conversion() != Conversion.NONE && type != DocValuesType.NUMERIC) {
                throw new IllegalArgumentException("the conversion [" + spec.conversion().externalName() + "] needs an integer column");
            }
            bindings.add(new ColumnBinding(spec.field(), number++, column.path(), type, spec.conversion()));
        }
        if (IndexSettings.MODE.get(indexSettings).isColumnar()) {
            bindings.add(ColumnBinding.synthesizedId(number));
        }
        return bindings;
    }

    private static DocValuesType docValuesType(ParquetFileInfo.LeafColumn column) {
        return switch (column.physicalType()) {
            case INT32, INT64 -> DocValuesType.NUMERIC;
            case BINARY -> DocValuesType.BINARY;
            case BOOLEAN, INT96, FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY -> throw new IllegalArgumentException(
                "column [" + column.path() + "] has the physical type [" + column.physicalType() + "], not supported yet"
            );
        };
    }
}
