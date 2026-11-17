/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/** parquet-java leg of the hive-partition parity sweep — the only format where the two fixed bugs were reachable. */
public class ParquetHivePartitionParityIT extends AbstractHivePartitionParityIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected String extension() {
        return "parquet";
    }

    @Override
    protected void writeFlat(Path file, List<int[]> rows) throws IOException {
        writeParquet(
            file,
            "message t { required int64 id; required binary region (UTF8); required binary tier (UTF8); "
                + "required int32 w; required int32 x; required int32 y; required int32 z; }",
            rows.size(),
            1024,
            (g, i) -> {
                int[] r = rows.get(i);
                g.add("id", (long) r[2]);
                g.add("region", REGION[r[0]]);
                g.add("tier", TIER[r[1]]);
                g.add("w", r[3]);
                g.add("x", r[4]);
                g.add("y", r[5]);
                g.add("z", r[6]);
            }
        );
    }

    @Override
    protected void writePartitioned(Path file, List<int[]> rows) throws IOException {
        writeParquet(
            file,
            "message t { required int64 id; required int32 w; required int32 x; required int32 y; required int32 z; }",
            rows.size(),
            1024,
            (g, i) -> {
                int[] r = rows.get(i);
                g.add("id", (long) r[2]);
                g.add("w", r[3]);
                g.add("x", r[4]);
                g.add("y", r[5]);
                g.add("z", r[6]);
            }
        );
    }
}
