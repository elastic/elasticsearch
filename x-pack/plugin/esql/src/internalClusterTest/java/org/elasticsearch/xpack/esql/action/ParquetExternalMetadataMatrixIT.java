/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.example.data.Group;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * Parquet binding of the standard-metadata matrix. Parquet is the only {@code ColumnExtractorAware}
 * reader, which means its {@code _rowPosition} channel is the deferred-extraction-encoded form
 * ({@code (extractorId << LOCAL_POSITION_BITS) | physicalPosition}); {@code ExternalRowIdentity}
 * masks the high bits off before composing {@code _id}. This subclass exercises that masked-compose
 * path; the shared base assertion is the opaque-token contract (distinct, non-negative, increasing
 * in file order, common location prefix) since each reader's token form is format-defined.
 *
 * <p>The existing {@code iceberg-fixtures/standalone/employees.parquet} is 100 rows with a complex
 * schema and no keyword {@code host_ip} column, so it cannot back the 3-row matrix fixture; this
 * writes a fresh 3-row file with the canonical {@code emp_no/first_name/host_ip} shape instead.
 */
public class ParquetExternalMetadataMatrixIT extends AbstractExternalMetadataMatrixIT {

    @Override
    protected String format() {
        return "parquet";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir) throws Exception {
        // int32 -> INTEGER, binary (UTF8) -> KEYWORD. A large row-group size keeps the three rows in one
        // row group, so they sit in the file at offsets 0,1,2.
        String[] firstNames = { "Alice", "Bob", "Carol" };
        String[] hostIps = { "10.0.0.1", "10.0.0.2", "10.0.0.3" };
        Path file = dir.resolve("employees.parquet");
        writeParquet(
            file,
            "message employees { required int32 emp_no; required binary first_name (UTF8); required binary host_ip (UTF8); }",
            3,
            1024 * 1024,
            (Group g, int i) -> {
                g.add("emp_no", i + 1);
                g.add("first_name", firstNames[i]);
                g.add("host_ip", hostIps[i]);
            }
        );
        return StoragePath.fileUri(file);
    }
}
