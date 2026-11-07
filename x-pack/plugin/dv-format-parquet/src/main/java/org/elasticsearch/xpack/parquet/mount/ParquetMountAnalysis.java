/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.mount;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.xpack.parquet.docvalues.ParquetFileInfo;
import org.elasticsearch.xpack.parquet.docvalues.ParquetFooterReader;
import org.elasticsearch.xpack.parquet.docvalues.ParquetObject;
import org.elasticsearch.xpack.parquet.docvalues.RepositoryLocation;
import org.elasticsearch.xpack.parquet.docvalues.RepositoryParquetObject;
import org.elasticsearch.xpack.parquet.mount.MountPlanner.IndexPlan;
import org.elasticsearch.xpack.parquet.mount.MountPlanner.Piece;
import org.elasticsearch.xpack.parquet.mount.ParquetMappingDeriver.DerivedMapping;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What a mount would do with its files: the footers of the files, the mapping that their schema gives, and how their row groups fill
 * the backing indices. A preview returns this and creates nothing, and a mount creates the indices from it. It reads one footer for
 * each file and no data, and it blocks on the repository, so it must run on a thread that may block.
 */
public record ParquetMountAnalysis(ParquetMountSpec spec, List<ParquetFileInfo> files, DerivedMapping mapping, List<IndexPlan> plan) {

    /** Reads the footers and works out the mapping and the plan. */
    public static ParquetMountAnalysis analyze(ParquetMountSpec spec, RepositoriesService repositories, ProjectId projectId)
        throws IOException {
        List<ParquetFileInfo> files = new ArrayList<>();
        for (String path : spec.paths()) {
            ParquetObject object;
            try {
                object = RepositoryParquetObject.open(repositories, projectId, new RepositoryLocation(spec.repository(), path));
            } catch (UncheckedIOException e) {
                if (e.getCause() instanceof NoSuchFileException) {
                    throw new ResourceNotFoundException("file [{}] does not exist in repository [{}]", e, path, spec.repository());
                }
                throw e;
            }
            files.add(ParquetFooterReader.read(object));
        }
        DerivedMapping mapping = ParquetMappingDeriver.derive(files.get(0), spec.overrides());
        List<String> columns = mapping.fields().stream().map(ParquetMappingDeriver.DerivedField::column).toList();
        for (int i = 1; i < files.size(); i++) {
            files.get(0).requireCompatibleWith(files.get(i), columns);
        }
        return new ParquetMountAnalysis(spec, List.copyOf(files), mapping, MountPlanner.plan(files, spec.maxRowsPerIndex()));
    }

    public long rows() {
        return files.stream().mapToLong(ParquetFileInfo::rowCount).sum();
    }

    /** The entries of {@code index.parquet.pieces} for the index of the plan. */
    public List<String> pieces(IndexPlan index) {
        List<String> pieces = new ArrayList<>();
        for (Piece piece : index.pieces()) {
            pieces.add(spec.paths().get(piece.file()) + "#" + piece.firstRowGroup() + "+" + piece.rowGroupCount());
        }
        return pieces;
    }

    /** The preview: what the files hold, the mapping, the columns without a field, and the indices that a mount would create. */
    public Map<String, Object> toPreview() {
        Map<String, Object> preview = new LinkedHashMap<>();
        preview.put("row_count", rows());
        List<Object> fileEntries = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            ParquetFileInfo file = files.get(i);
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("path", spec.paths().get(i));
            entry.put("rows", file.rowCount());
            entry.put("row_groups", file.rowGroups().size());
            entry.put("size_in_bytes", file.length());
            if (file.createdBy() != null) {
                entry.put("created_by", file.createdBy());
            }
            fileEntries.add(entry);
        }
        preview.put("files", fileEntries);
        Map<String, Object> mappingEntry = new LinkedHashMap<>();
        mappingEntry.put("properties", mapping.properties());
        preview.put("mapping", mappingEntry);
        List<Object> unmapped = new ArrayList<>();
        for (ParquetMappingDeriver.UnmappedColumn column : mapping.unmapped()) {
            unmapped.add(Map.of("column", column.column(), "reason", column.reason()));
        }
        preview.put("unmapped", unmapped);
        List<Object> indices = new ArrayList<>();
        for (IndexPlan index : plan) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("rows", index.rows());
            List<Object> pieces = new ArrayList<>();
            for (Piece piece : index.pieces()) {
                pieces.add(
                    Map.of(
                        "path",
                        spec.paths().get(piece.file()),
                        "first_row_group",
                        piece.firstRowGroup(),
                        "row_group_count",
                        piece.rowGroupCount(),
                        "rows",
                        piece.rows()
                    )
                );
            }
            entry.put("pieces", pieces);
            indices.add(entry);
        }
        preview.put("indices", indices);
        return preview;
    }
}
