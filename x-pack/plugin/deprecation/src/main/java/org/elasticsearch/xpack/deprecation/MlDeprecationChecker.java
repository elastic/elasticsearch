/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MlDeprecationChecker implements DeprecationChecker {

    static Optional<DeprecationIssue> checkDataFeedQuery(DatafeedConfig datafeedConfig, NamedXContentRegistry xContentRegistry) {
        List<String> deprecations = datafeedConfig.getQueryDeprecations(xContentRegistry);
        if (deprecations.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Datafeed [" + datafeedConfig.getId() + "] uses deprecated query options",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html#breaking_70_search_changes",
                deprecations.toString()));
        }
    }

    static Optional<DeprecationIssue> checkDataFeedAggregations(DatafeedConfig datafeedConfig, NamedXContentRegistry xContentRegistry) {
        List<String> deprecations = datafeedConfig.getAggDeprecations(xContentRegistry);
        if (deprecations.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new DeprecationIssue(DeprecationIssue.Level.WARNING,
                "Datafeed [" + datafeedConfig.getId() + "] uses deprecated aggregation options",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-7.0.html" +
                    "#breaking_70_aggregations_changes", deprecations.toString()));
        }
    }

    @Override
    public boolean enabled(Settings settings) {
        return XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
    }

    @Override
    public void check(Components components, ActionListener<CheckResult> deprecationIssueListener) {
        if (XPackSettings.MACHINE_LEARNING_ENABLED.get(components.settings()) == false) {
            deprecationIssueListener.onResponse(new CheckResult(getName(), Collections.emptyList()));
            return;
        }
        components.client().execute(
            GetDatafeedsAction.INSTANCE,
            new GetDatafeedsAction.Request(GetDatafeedsAction.ALL), ActionListener.wrap(
                datafeedsResponse -> {
                    List<DeprecationIssue> issues = new ArrayList<>();
                    for (DatafeedConfig df : datafeedsResponse.getResponse().results()) {
                        checkDataFeedAggregations(df, components.xContentRegistry()).ifPresent(issues::add);
                        checkDataFeedQuery(df, components.xContentRegistry()).ifPresent(issues::add);
                    }
                    deprecationIssueListener.onResponse(new CheckResult(getName(), issues));
                },
                deprecationIssueListener::onFailure
            )
        );
    }

    @Override
    public String getName() {
        return "ml_settings";
    }
}
