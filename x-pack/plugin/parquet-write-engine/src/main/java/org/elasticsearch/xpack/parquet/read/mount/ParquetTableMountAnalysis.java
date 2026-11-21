/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.read.mount;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.parquet.read.dv.ParquetFileInfo;
import org.elasticsearch.xpack.parquet.read.mount.ParquetMappingDeriver.DerivedMapping;
import org.elasticsearch.xpack.parquet.read.repository.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.read.repository.ParquetFiles;
import org.elasticsearch.xpack.parquet.read.repository.RepositoryLocation;
import org.elasticsearch.xpack.parquet.write.TableReaders;
import org.elasticsearch.xpack.parquet.write.table.TableReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What a mount of an Iceberg table would do: the current snapshot of the table, and the mapping that the schema of its data files gives.
 * The mapping comes from the footer of the newest data file, so a table needs at least one data file to be mounted. The index follows the
 * table afterwards, so its files are not listed in its settings. It blocks on the repository, so it must run on a thread that may block.
 *
 * @param version the version of the table that the analysis read
 * @param files   the number of data files of the current snapshot
 * @param rows    the number of rows of the current snapshot
 * @param sample  the data file whose schema gives the mapping, below the base path of the repository
 */
public record ParquetTableMountAnalysis(ParquetMountSpec spec, long version, int files, long rows, String sample, DerivedMapping mapping) {

    /** Reads the table and the footer of its newest data file, and derives the mapping. */
    public static ParquetTableMountAnalysis analyze(
        ParquetMountSpec spec,
        TableReaders tableReaders,
        ParquetFiles parquetFiles,
        ProjectId projectId
    ) throws IOException {
        final List<String> tablePath = ParquetBackedSettings.tablePathSegments(spec.table());
        final TableReader reader = tableReaders.open(projectId, spec.repository(), tablePath);
        final TableReader.Contents contents = reader.read();
        if (contents.version() == 0) {
            throw new ResourceNotFoundException("there is no Iceberg table at [{}] in [{}]", spec.table(), spec.repository());
        }
        if (contents.files().isEmpty()) {
            throw new IllegalArgumentException(
                "the table at [" + spec.table() + "] has no data file yet, and the mapping of a mount comes from the schema of one"
            );
        }
        TableReader.DataFileEntry newest = contents.files().get(0);
        long rows = 0;
        for (TableReader.DataFileEntry file : contents.files()) {
            rows += file.rows();
            if (file.sequenceNumber() > newest.sequenceNumber()) {
                newest = file;
            }
        }
        final String sample = String.join("/", tablePath) + "/" + newest.pathInTable(reader.location());
        final RepositoryLocation location = new RepositoryLocation(spec.repository(), sample);
        final StorageObject object = parquetFiles.open(location, projectId, newest.length());
        final ParquetFileInfo info;
        try {
            info = ParquetFileInfo.read(location.toString(), object);
        } finally {
            ParquetFiles.close(object);
        }
        final DerivedMapping mapping = ParquetMappingDeriver.derive(info, spec.overrides());
        return new ParquetTableMountAnalysis(spec, contents.version(), contents.files().size(), rows, sample, mapping);
    }

    /** The preview: the table, the mapping, and the columns without a field. */
    public Map<String, Object> toPreview() {
        final Map<String, Object> preview = new LinkedHashMap<>();
        preview.put("table", spec.table());
        preview.put("table_version", version);
        preview.put("data_files", files);
        preview.put("row_count", rows);
        preview.put("mapping_from", sample);
        final Map<String, Object> mappingEntry = new LinkedHashMap<>();
        mappingEntry.put("properties", mapping.properties());
        preview.put("mapping", mappingEntry);
        final List<Object> unmapped = new ArrayList<>();
        for (ParquetMappingDeriver.UnmappedColumn column : mapping.unmapped()) {
            unmapped.add(Map.of("column", column.column(), "reason", column.reason()));
        }
        preview.put("unmapped", unmapped);
        return preview;
    }
}
