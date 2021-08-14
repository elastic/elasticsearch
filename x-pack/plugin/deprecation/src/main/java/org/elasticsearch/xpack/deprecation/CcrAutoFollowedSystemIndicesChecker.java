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
        return new DeprecationIssue(DeprecationIssue.Level.WARNING,
            "An auto followed index follows a remote system index",
            "https://www.elastic.co/guide/en/elasticsearch/reference/7.13/migrating-7.14.html#breaking_714_ccr_changes",
            "Auto followed index [" + localIndexName
                + "] follows a remote system index and this behaviour will change in the next major version.",
            false, null
        );
    }

    @Override
    public String getName() {
        return "ccr_auto_followed_system_indices";
    }
}
