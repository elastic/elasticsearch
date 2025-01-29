/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
import java.util.List;

class TransformDeprecationChecker implements DeprecationChecker {

    public static final String TRANSFORM_DEPRECATION_KEY = "transform_settings";
    private final List<TransformConfig> transformConfigs;

    TransformDeprecationChecker(List<TransformConfig> transformConfigs) {
        this.transformConfigs = transformConfigs;
    }

    @Override
    public boolean enabled(Settings settings) {
        // always enabled
        return true;
    }

    @Override
    public void check(Components components, ActionListener<CheckResult> deprecationIssueListener) {
        ActionListener.completeWith(deprecationIssueListener, () -> {
            List<DeprecationIssue> allIssues = new ArrayList<>();
            for (var config : transformConfigs) {
                allIssues.addAll(config.checkForDeprecations(components.xContentRegistry()));
            }
            return new CheckResult(getName(), allIssues);
        });
    }

    @Override
    public String getName() {
        return TRANSFORM_DEPRECATION_KEY;
    }
}
