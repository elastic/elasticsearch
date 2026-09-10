/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.List;
import java.util.Objects;

/**
 * Auto-detecting partition detector that tries Hive-style detection first,
 * then falls back to template-based detection if a path template is configured.
 */
public final class AutoPartitionDetector implements PartitionDetector {

    private final PartitionConfig partitionConfig;

    AutoPartitionDetector(PartitionConfig partitionConfig) {
        this.partitionConfig = Objects.requireNonNull(partitionConfig, "partitionConfig cannot be null");
    }

    public static PartitionDetector fromConfig(PartitionConfig config) {
        return new AutoPartitionDetector(config);
    }

    @Override
    public String name() {
        return "auto";
    }

    @Override
    public PartitionMetadata detect(List<StorageEntry> files) {
        // Try Hive first
        PartitionMetadata hiveResult = HivePartitionDetector.INSTANCE.detect(files);
        if (hiveResult.isEmpty() == false) {
            return hiveResult;
        }

        // Fall back to template if configured
        // Same grammar guard as GlobExpander.resolveDetector: TemplatePartitionDetector's constructor rejects a
        // template naming no whole-segment {name} placeholders, and this fallback is reachable with any stored value.
        String template = partitionConfig.pathTemplate();
        if (template != null && TemplatePartitionDetector.parseTemplateColumns(template).isEmpty() == false) {
            TemplatePartitionDetector templateDetector = new TemplatePartitionDetector(template);
            return templateDetector.detect(files);
        }

        return PartitionMetadata.EMPTY;
    }
}
