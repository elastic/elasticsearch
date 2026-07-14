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

/** parquet-java leg of the boolean hive-partition cast guard. */
public class ParquetHivePartitionBooleanCastIT extends AbstractHivePartitionBooleanCastIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(ParquetDataSourcePlugin.class);
    }

    @Override
    protected String extension() {
        return "parquet";
    }

    @Override
    protected void writeIdRows(Path file, List<Integer> ids) throws IOException {
        writeParquet(file, "message t { required int32 id; }", ids.size(), 1024, (g, i) -> g.add("id", ids.get(i)));
    }
}
