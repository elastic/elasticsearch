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

final class TransformConfigLinter {

    static List<String> getWarnings(Function function, SourceConfig sourceConfig, SyncConfig syncConfig) {
        if (syncConfig == null) {
            return Collections.emptyList();
        }

        List<String> warnings = new ArrayList<>();
        Map<String, Object> scriptBasedRuntimeFieldNames = sourceConfig.getScriptBasedRuntimeMappings();
        List<String> performanceCriticalFields = function.getPerformanceCriticalFields();
        if (performanceCriticalFields.stream().allMatch(scriptBasedRuntimeFieldNames::containsKey)) {
            String message = "all the group-by fields are script-based runtime fields, "
                + "this transform might run slowly, please check your configuration.";
            warnings.add(message);
        }
        if (scriptBasedRuntimeFieldNames.containsKey(syncConfig.getField())) {
            String message = "sync time field is a script-based runtime field, "
                + "this transform might run slowly, please check your configuration.";
            warnings.add(message);
        }
        Function.ChangeCollector changeCollector = function.buildChangeCollector(syncConfig.getField());
        if (changeCollector.isOptimized() == false) {
            String message = "could not find any optimizations for continuous execution, "
                + "this transform might run slowly, please check your configuration.";
            warnings.add(message);
        }
        return warnings;
    }

    private TransformConfigLinter() {}
}
