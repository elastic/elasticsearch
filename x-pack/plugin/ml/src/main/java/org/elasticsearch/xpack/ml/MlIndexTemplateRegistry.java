/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
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

    private static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/";
    private static final String ANOMALY_DETECTION_PATH = ROOT_RESOURCE_PATH + "anomalydetection/";
    private static final String VERSION_PATTERN = "xpack.ml.version";
    private static final String VERSION_ID_PATTERN = "xpack.ml.version.id";
    private static final String INDEX_LIFECYCLE_NAME = "xpack.ml.index.lifecycle.name";
    private static final String INDEX_LIFECYCLE_ROLLOVER_ALIAS = "xpack.ml.index.lifecycle.rollover_alias";

    private static final IndexTemplateConfig ANOMALY_DETECTION_RESULTS_TEMPLATE = anomalyDetectionResultsTemplate();

    private static final IndexTemplateConfig ANOMALY_DETECTION_STATE_TEMPLATE = stateTemplate();

    private static final IndexTemplateConfig META_TEMPLATE = new IndexTemplateConfig(MlMetaIndex.indexName(),
        ROOT_RESOURCE_PATH + "meta_index_template.json", Version.CURRENT.id, VERSION_PATTERN,
        Collections.singletonMap(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id)));

    private static final IndexTemplateConfig NOTIFICATIONS_TEMPLATE = new IndexTemplateConfig(NotificationsIndex.NOTIFICATIONS_INDEX,
        ROOT_RESOURCE_PATH + "notifications_index_template.json", Version.CURRENT.id, VERSION_PATTERN,
        Collections.singletonMap(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id)));

    private static final IndexTemplateConfig CONFIG_TEMPLATE = configTemplate();

    private static final IndexTemplateConfig INFERENCE_TEMPLATE = new IndexTemplateConfig(InferenceIndexConstants.LATEST_INDEX_NAME,
        ROOT_RESOURCE_PATH + "inference_index_template.json", Version.CURRENT.id, VERSION_PATTERN,
        Collections.singletonMap(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id)));

    private static final IndexTemplateConfig STATS_TEMPLATE = statsTemplate();

    private static final String ML_SIZE_BASED_ILM_POLICY_NAME = "ml-size-based-ilm-policy";
    private static final LifecyclePolicyConfig ML_SIZE_BASED_ILM_POLICY =
        new LifecyclePolicyConfig(ML_SIZE_BASED_ILM_POLICY_NAME, ROOT_RESOURCE_PATH + "size_based_ilm_policy.json");

    private static IndexTemplateConfig configTemplate() {
        Map<String, String> variables = new HashMap<>();
        variables.put(VERSION_ID_PATTERN, String.valueOf(Version.CURRENT.id));
        variables.put("xpack.ml.config.max_result_window",
            String.valueOf(AnomalyDetectorsIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW));
        variables.put("xpack.ml.config.mappings", MlConfigIndex.mapping());

        return new IndexTemplateConfig(MlConfigIndex.indexName(),
            ROOT_RESOURCE_PATH + "config_index_template.json",
            Version.CURRENT.id, VERSION_PATTERN,
            variables);
    }

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
            CONFIG_TEMPLATE,
            INFERENCE_TEMPLATE,
            META_TEMPLATE,
            NOTIFICATIONS_TEMPLATE,
            STATS_TEMPLATE);
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    @Override
    protected List<IndexTemplateConfig> getTemplateConfigs() {
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
