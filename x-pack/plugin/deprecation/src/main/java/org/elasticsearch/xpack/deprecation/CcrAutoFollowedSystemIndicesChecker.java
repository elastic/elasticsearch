/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ccr.CcrAutoFollowInfoFetcher;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.XPackSettings.CCR_ENABLED_SETTING;

public class CcrAutoFollowedSystemIndicesChecker implements DeprecationChecker {
    @Override
    public boolean enabled(Settings settings) {
        return CCR_ENABLED_SETTING.get(settings);
    }

    @Override
    public void check(Components components, ActionListener<CheckResult> deprecationIssueListener) {
        CcrAutoFollowInfoFetcher.getAutoFollowedSystemIndices(
            components.client(),
            components.clusterState(),
            deprecationIssueListener.map(autoFollowedSystemIndices -> {
                final List<DeprecationIssue> deprecationIssues = autoFollowedSystemIndices.stream()
                    .map(this::createDeprecationIssue)
                    .collect(Collectors.toList());
                return new CheckResult(getName(), deprecationIssues);
            })
        );
    }

    private DeprecationIssue createDeprecationIssue(String localIndexName) {
        return new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Automatically following system indices is deprecated",
            "https://ela.st/es-deprecation-7-auto-follow-system-index",
            "Index ["
                + localIndexName
                + "] follows a remote system index. In 8.0.0, follower indices will not be created for remote "
                + "system indices "
                + "that match an auto-follow pattern.",
            false,
            null
        );
    }

    @Override
    public String getName() {
        return "ccr_auto_followed_system_indices";
    }
}
