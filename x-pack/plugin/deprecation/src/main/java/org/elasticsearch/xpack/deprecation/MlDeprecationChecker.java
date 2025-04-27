/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
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

import static org.elasticsearch.xpack.core.ml.MachineLearningField.MIN_REPORTED_SUPPORTED_SNAPSHOT_VERSION;

public class MlDeprecationChecker implements DeprecationChecker {

    static Optional<DeprecationIssue> checkDataFeedQuery(DatafeedConfig datafeedConfig, NamedXContentRegistry xContentRegistry) {
        List<String> deprecations = datafeedConfig.getQueryDeprecations(xContentRegistry);
        if (deprecations.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Datafeed [" + datafeedConfig.getId() + "] uses deprecated query options",
                    "https://ela.st/es-deprecation-7-data-feed-query",
                    deprecations.toString(),
                    false,
                    null
                )
            );
        }
    }

    static Optional<DeprecationIssue> checkDataFeedAggregations(DatafeedConfig datafeedConfig, NamedXContentRegistry xContentRegistry) {
        List<String> deprecations = datafeedConfig.getAggDeprecations(xContentRegistry);
        if (deprecations.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "Datafeed [" + datafeedConfig.getId() + "] uses deprecated aggregation options",
                    "https://ela.st/es-deprecation-7-data-feed-aggregation",
                    deprecations.toString(),
                    false,
                    null
                )
            );
        }
    }

    static Optional<DeprecationIssue> checkModelSnapshot(ModelSnapshot modelSnapshot) {
        if (modelSnapshot.getMinVersion().before(MIN_REPORTED_SUPPORTED_SNAPSHOT_VERSION)) {
            StringBuilder details = new StringBuilder(
                String.format(
                    Locale.ROOT,
                    "Delete model snapshot [%s] or update it to %s or greater.",
                    modelSnapshot.getSnapshotId(),
                    MIN_REPORTED_SUPPORTED_SNAPSHOT_VERSION
                )
            );
            if (modelSnapshot.getLatestRecordTimeStamp() != null) {
                details.append(
                    String.format(
                        Locale.ROOT,
                        " The model snapshot's latest record timestamp is [%s].",
                        XContentElasticsearchExtension.DEFAULT_FORMATTER.format(modelSnapshot.getLatestRecordTimeStamp().toInstant())
                    )
                );
            }
            return Optional.of(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    String.format(
                        Locale.ROOT,
                        // Important: the Kibana upgrade assistant expects this to match the pattern /[Mm]odel snapshot/
                        // and if it doesn't then the expected "Fix" button won't appear for this deprecation.
                        "Model snapshot [%s] for job [%s] has an obsolete minimum version [%s].",
                        modelSnapshot.getSnapshotId(),
                        modelSnapshot.getJobId(),
                        modelSnapshot.getMinVersion()
                    ),
                    "https://ela.st/es-deprecation-8-model-snapshot-version",
                    details.toString(),
                    false,
                    Map.of("job_id", modelSnapshot.getJobId(), "snapshot_id", modelSnapshot.getSnapshotId())
                )
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

        ActionListener<Void> getModelSnaphots = deprecationIssueListener.delegateFailureAndWrap(
            (delegate, _unused) -> components.client()
                .execute(GetModelSnapshotsAction.INSTANCE, getModelSnapshots, delegate.delegateFailureAndWrap((l, modelSnapshots) -> {
                    modelSnapshots.getResources()
                        .results()
                        .forEach(modelSnapshot -> checkModelSnapshot(modelSnapshot).ifPresent(issues::add));
                    l.onResponse(new CheckResult(getName(), issues));
                }))
        );

        components.client()
            .execute(
                GetDatafeedsAction.INSTANCE,
                new GetDatafeedsAction.Request(GetDatafeedsAction.ALL),
                getModelSnaphots.delegateFailureAndWrap((delegate, datafeedsResponse) -> {
                    for (DatafeedConfig df : datafeedsResponse.getResponse().results()) {
                        checkDataFeedAggregations(df, components.xContentRegistry()).ifPresent(issues::add);
                        checkDataFeedQuery(df, components.xContentRegistry()).ifPresent(issues::add);
                    }
                    delegate.onResponse(null);
                })
            );
    }

    @Override
    public String getName() {
        return "ml_settings";
    }
}
