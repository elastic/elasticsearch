/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Default {@link SplitProvider} for file-based sources.
 * Converts each file in the {@link FileSet} into a {@link FileSplit},
 * applying partition pruning when filter hints and partition metadata are available.
 */
public class FileSplitProvider implements SplitProvider {

    @Override
    public List<ExternalSplit> discoverSplits(SplitDiscoveryContext context) {
        FileSet fileSet = context.fileSet();
        if (fileSet == null || fileSet.isResolved() == false) {
            return List.of();
        }

        PartitionMetadata partitionInfo = context.partitionInfo();
        Map<String, Object> config = context.config();
        List<ExternalSplit> splits = new ArrayList<>();

        for (StorageEntry entry : fileSet.files()) {
            StoragePath filePath = entry.path();

            Map<String, Object> partitionValues = Map.of();
            if (partitionInfo != null && partitionInfo.isEmpty() == false) {
                Map<String, Object> filePartitions = partitionInfo.filePartitionValues().get(filePath);
                if (filePartitions != null) {
                    partitionValues = filePartitions;
                }
            }

            String objectName = filePath.objectName();
            String format = null;
            if (objectName != null) {
                int lastDot = objectName.lastIndexOf('.');
                if (lastDot >= 0 && lastDot < objectName.length() - 1) {
                    format = objectName.substring(lastDot);
                }
            }

            splits.add(new FileSplit("file", filePath, 0, entry.length(), format, config, partitionValues));
        }

        return List.copyOf(splits);
    }
}
