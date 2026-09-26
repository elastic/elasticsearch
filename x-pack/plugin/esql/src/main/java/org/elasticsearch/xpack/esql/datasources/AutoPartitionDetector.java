/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.TemplatePartitionDetector.TemplateSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

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
    public PartitionMetadata detect(List<StorageEntry> files, Consumer<String> warningSink) {
        Objects.requireNonNull(warningSink, "warningSink: a null sink would fall back to HeaderWarning off the request thread");
        // Try Hive first. A dotted or empty parent (snapshot=2024.05/) is a hive column and would hide a template
        // the user set for the tail ({year}/{month}). When every hive key sits above that tail, the template wins.
        String template = partitionConfig.pathTemplate();
        boolean templateUsable = template != null && TemplatePartitionDetector.parseTemplateColumns(template).isEmpty() == false;
        List<String> hiveWarnings = new ArrayList<>();
        PartitionMetadata hiveResult = HivePartitionDetector.INSTANCE.detect(files, hiveWarnings::add);
        if (hiveResult.isEmpty() == false && templateUsable && hiveKeysOnlyOutsideTemplate(files, template)) {
            PartitionMetadata templated = new TemplatePartitionDetector(template).detect(files, warningSink);
            if (templated.isEmpty() == false) {
                return templated;
            }
        }
        if (hiveResult.isEmpty() == false) {
            hiveWarnings.forEach(warningSink);
            return hiveResult;
        }

        // Fall back to template if configured
        // Same grammar guard as GlobExpander.resolveDetector: TemplatePartitionDetector's constructor rejects a
        // template naming no whole-segment {name} placeholders, and this fallback is reachable with any stored value.
        if (templateUsable) {
            return new TemplatePartitionDetector(template).detect(files, warningSink);
        }

        return PartitionMetadata.EMPTY;
    }

    /**
     * Whether every hive {@code key=value} directory is above the template's right-aligned window. A key inside the
     * window means the tail itself is hive, and hive stays the winner. No hive key, or a file too shallow for the
     * template, is not this case.
     */
    private static boolean hiveKeysOnlyOutsideTemplate(List<StorageEntry> files, String template) {
        List<TemplateSegment> segments = TemplatePartitionDetector.parseTemplate(template);
        if (segments.isEmpty()) {
            return false;
        }
        boolean sawOutside = false;
        for (StorageEntry file : files) {
            List<String> dirs = HivePartitionDetector.directorySegments(file.path().path());
            if (dirs.size() < segments.size()) {
                return false;
            }
            int windowStart = dirs.size() - segments.size();
            for (int i = 0; i < dirs.size(); i++) {
                if (HivePartitionDetector.segmentKey(dirs.get(i)) == null) {
                    continue;
                }
                if (i >= windowStart) {
                    return false;
                }
                sawOutside = true;
            }
        }
        return sawOutside;
    }
}
