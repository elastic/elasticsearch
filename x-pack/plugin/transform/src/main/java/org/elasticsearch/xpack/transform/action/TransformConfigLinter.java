/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.transform.transforms.Function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link TransformConfigLinter} analyzes transform config in order to find issues that might be of interest to the transform user.
 *
 * These issues are reported as warnings. They do *not* fail the request (like validations do) but instead provide hints that may be then
 * logged or displayed in the UI.
 */
final class TransformConfigLinter {

    private static final String GROUP_BY_FIELDS_ARE_RUNTIME_FIELDS =
        "all the group-by fields are script-based runtime fields, this transform might run slowly, please check your configuration.";
    private static final String SYNC_FIELD_IS_RUNTIME_FIELD =
        "sync time field is a script-based runtime field, this transform might run slowly, please check your configuration.";
    private static final String NOT_OPTIMIZED =
        "could not find any optimizations for continuous execution, this transform might run slowly, please check your configuration.";

    /**
     * Gets the list of warnings for the given config.
     *
     * @param function transform function
     * @param sourceConfig source config
     * @param syncConfig synchronization config when the transform is continuous or {@code null} otherwise
     * @return list of warnings
     */
    static List<String> getWarnings(Function function, SourceConfig sourceConfig, SyncConfig syncConfig) {
        if (syncConfig == null) {
            return Collections.emptyList();
        }

        List<String> warnings = new ArrayList<>();
        Map<String, Object> scriptBasedRuntimeFieldNames = sourceConfig.getScriptBasedRuntimeMappings();
        List<String> performanceCriticalFields = function.getPerformanceCriticalFields();
        if (performanceCriticalFields.stream().allMatch(scriptBasedRuntimeFieldNames::containsKey)) {
            warnings.add(GROUP_BY_FIELDS_ARE_RUNTIME_FIELDS);
        }
        if (scriptBasedRuntimeFieldNames.containsKey(syncConfig.getField())) {
            warnings.add(SYNC_FIELD_IS_RUNTIME_FIELD);
        }
        Function.ChangeCollector changeCollector = function.buildChangeCollector(syncConfig.getField());
        if (changeCollector.isOptimized() == false) {
            warnings.add(NOT_OPTIMIZED);
        }
        return warnings;
    }

    private TransformConfigLinter() {}
}
