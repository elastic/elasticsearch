/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.List;
import java.util.Map;

/**
 * Auto-detecting partition detector that tries Hive-style detection first,
 * then falls back to template-based detection if a path template is configured.
 */
final class AutoPartitionDetector implements PartitionDetector {

    private final PartitionConfig partitionConfig;

    AutoPartitionDetector(PartitionConfig partitionConfig) {
        this.partitionConfig = partitionConfig;
    }

    static PartitionDetector fromConfig(PartitionConfig config) {
        if (config == null) {
            return HivePartitionDetector.INSTANCE;
        }
        return new AutoPartitionDetector(config);
    }

    @Override
    public String name() {
        return "auto";
    }

    @Override
    public PartitionMetadata detect(List<StorageEntry> files, Map<String, Object> config) {
        // Try Hive first
        PartitionMetadata hiveResult = HivePartitionDetector.INSTANCE.detect(files, config);
        if (hiveResult.isEmpty() == false) {
            return hiveResult;
        }

        // Fall back to template if configured
        String template = partitionConfig != null ? partitionConfig.pathTemplate() : null;
        if (template != null && template.isEmpty() == false) {
            TemplatePartitionDetector templateDetector = new TemplatePartitionDetector(template);
            return templateDetector.detect(files, config);
        }

        return PartitionMetadata.EMPTY;
    }
}
