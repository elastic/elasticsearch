/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndexFields;
import org.elasticsearch.xpack.core.ml.notifications.NotificationsIndex;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MlIndexTemplateRegistry extends IndexTemplateRegistry {

    /**
     * The version that the ML index templates were switched from legacy templates to composable templates.
     */
    public static final Version COMPOSABLE_TEMPLATE_SWITCH_VERSION = Version.V_7_14_0;

    private static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/";
    private static final String ANOMALY_DETECTION_PATH = ROOT_RESOURCE_PATH + "anomalydetection/";
    private static final String VERSION_PATTERN = "xpack.ml.version";
    private static final String VERSION_ID_PATTERN = "xpack.ml.version.id";
    private static final String INDEX_LIFECYCLE_NAME = "xpack.ml.index.lifecycle.name";
    private static final String INDEX_LIFECYCLE_ROLLOVER_ALIAS = "xpack.ml.index.lifecycle.rollover_alias";

    private static final IndexTemplateConfig ANOMALY_DETECTION_RESULTS_TEMPLATE = anomalyDetectionResultsTemplate();

    private static final IndexTemplateConfig ANOMALY_DETECTION_STATE_TEMPLATE = stateTemplate();

    public static final IndexTemplateConfig NOTIFICATIONS_TEMPLATE = notificationsTemplate();
    public static final IndexTemplateConfig NOTIFICATIONS_LEGACY_TEMPLATE = notificationsLegacyTemplate();

    private static final IndexTemplateConfig STATS_TEMPLATE = statsTemplate();

    private static final String ML_SIZE_BASED_ILM_POLICY_NAME = "ml-size-based-ilm-policy";
    private static final LifecyclePolicyConfig ML_SIZE_BASED_ILM_POLICY =
        new LifecyclePolicyConfig(ML_SIZE_BASED_ILM_POLICY_NAME, ROOT_RESOURCE_PATH + "size_based_ilm_policy.json");

    private static IndexTemplateConfig stateTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put(INDEX_LIFECYCLE_NAME, ML_SIZE_BASED_ILM_POLICY_NAME);
        variables.put(INDEX_LIFECYCLE_ROLLOVER_ALIAS, AnomalyDetectorsIndex.jobStateIndexWriteAlias());

        return new IndexTemplateConfig(AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX,
            ANOMALY_DETECTION_PATH + "state_index_template.json",
            Version.CURRENT.id, VERSION_PATTERN,
            variables);
    }

    private static IndexTemplateConfig anomalyDetectionResultsTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.anomalydetection.results.mappings", AnomalyDetectorsIndex.resultsMapping());

        return new IndexTemplateConfig(AnomalyDetectorsIndex.jobResultsIndexPrefix(),
            ANOMALY_DETECTION_PATH + "results_index_template.json",
            Version.CURRENT.id, VERSION_PATTERN,
            variables);
    }

    private static IndexTemplateConfig notificationsTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.notifications.mappings", NotificationsIndex.mapping());

        return new IndexTemplateConfig(NotificationsIndex.NOTIFICATIONS_INDEX,
            ROOT_RESOURCE_PATH + "notifications_index_template.json",
            Version.CURRENT.id, VERSION_PATTERN,
            variables);
    }

    private static IndexTemplateConfig notificationsLegacyTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.notifications.mappings", NotificationsIndex.mapping());

        return new IndexTemplateConfig(NotificationsIndex.NOTIFICATIONS_INDEX,
            ROOT_RESOURCE_PATH + "notifications_index_legacy_template.json",
            Version.CURRENT.id, VERSION_PATTERN,
            variables);
    }

    private static IndexTemplateConfig statsTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.stats.mappings", MlStatsIndex.mapping());
        variables.put(INDEX_LIFECYCLE_NAME, ML_SIZE_BASED_ILM_POLICY_NAME);
        variables.put(INDEX_LIFECYCLE_ROLLOVER_ALIAS, MlStatsIndex.writeAlias());

        return new IndexTemplateConfig(MlStatsIndex.TEMPLATE_NAME,
            ROOT_RESOURCE_PATH + "stats_index_template.json",
            Version.CURRENT.id, VERSION_PATTERN,
            variables);
    }

    private final List<IndexTemplateConfig> templatesToUse;

    public MlIndexTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool, Client client,
                                   NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        templatesToUse = Arrays.asList(
            ANOMALY_DETECTION_RESULTS_TEMPLATE,
            ANOMALY_DETECTION_STATE_TEMPLATE,
            NOTIFICATIONS_TEMPLATE,
            STATS_TEMPLATE);
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    @Override
    protected List<IndexTemplateConfig> getComposableTemplateConfigs() {
        return templatesToUse;
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        return Collections.singletonList(ML_SIZE_BASED_ILM_POLICY);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.ML_ORIGIN;
    }
}
