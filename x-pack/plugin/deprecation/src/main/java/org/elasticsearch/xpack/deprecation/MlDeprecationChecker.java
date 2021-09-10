/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
                deprecations.toString(), false, null));
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
                    "#breaking_70_aggregations_changes", deprecations.toString(), false, null));
        }
    }

    static Optional<DeprecationIssue> checkModelSnapshot(ModelSnapshot modelSnapshot) {
        if (modelSnapshot.getMinVersion().before(Version.V_7_0_0)) {
            StringBuilder details = new StringBuilder(String.format(
                Locale.ROOT,
                "model snapshot [%s] for job [%s] supports minimum version [%s] and needs to be at least [%s].",
                modelSnapshot.getSnapshotId(),
                modelSnapshot.getJobId(),
                modelSnapshot.getMinVersion(),
                Version.V_7_0_0));
            if (modelSnapshot.getLatestRecordTimeStamp() != null) {
                details.append(String.format(
                    Locale.ROOT,
                    " The model snapshot's latest record timestamp is [%s]",
                    XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(modelSnapshot.getLatestRecordTimeStamp().getTime())
                ));
            }
            return Optional.of(new DeprecationIssue(DeprecationIssue.Level.CRITICAL,
                String.format(
                    Locale.ROOT,
                    "model snapshot [%s] for job [%s] needs to be deleted or upgraded",
                    modelSnapshot.getSnapshotId(),
                    modelSnapshot.getJobId()
                ),
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/ml-upgrade-job-model-snapshot.html",
                details.toString(),
                false,
                Map.of("job_id", modelSnapshot.getJobId(), "snapshot_id", modelSnapshot.getSnapshotId()))
            );
        }
        return Optional.empty();
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
        List<DeprecationIssue> issues = Collections.synchronizedList(new ArrayList<>());
        final GetModelSnapshotsAction.Request getModelSnapshots = new GetModelSnapshotsAction.Request("*", null);
        getModelSnapshots.setPageParams(new PageParams(0, 50));
        getModelSnapshots.setSort(ModelSnapshot.MIN_VERSION.getPreferredName());

        ActionListener<Void> getModelSnaphots = ActionListener.wrap(
            _unused -> components.client().execute(
                GetModelSnapshotsAction.INSTANCE,
                getModelSnapshots,
                ActionListener.wrap(
                    modelSnapshots -> {
                        modelSnapshots.getResources()
                            .results()
                            .forEach(modelSnapshot -> checkModelSnapshot(modelSnapshot)
                                .ifPresent(issues::add));
                        deprecationIssueListener.onResponse(new CheckResult(getName(), issues));
                    },
                    deprecationIssueListener::onFailure)
            ),
            deprecationIssueListener::onFailure);

        components.client().execute(
            GetDatafeedsAction.INSTANCE,
            new GetDatafeedsAction.Request(GetDatafeedsAction.ALL), ActionListener.wrap(
                datafeedsResponse -> {
                    for (DatafeedConfig df : datafeedsResponse.getResponse().results()) {
                        checkDataFeedAggregations(df, components.xContentRegistry()).ifPresent(issues::add);
                        checkDataFeedQuery(df, components.xContentRegistry()).ifPresent(issues::add);
                    }
                    getModelSnaphots.onResponse(null);
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
