/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.CategorizerState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.Quantiles;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.ml.job.results.Result;
import org.elasticsearch.xpack.ml.notifications.AuditMessage;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Registry for the ML index templates and settings
 */
public class MachineLearningTemplateRegistry  extends AbstractComponent implements ClusterStateListener {
    private static final String ASYNC = "async";

    private final Client client;
    private final ThreadPool threadPool;

    public static String [] TEMPLATE_NAMES = new String [] {Auditor.NOTIFICATIONS_INDEX, AnomalyDetectorsIndex.ML_META_INDEX,
            AnomalyDetectorsIndex.jobStateIndexName(), AnomalyDetectorsIndex.jobResultsIndexPrefix()};

    final AtomicBoolean putMlNotificationsIndexTemplateCheck = new AtomicBoolean(false);
    final AtomicBoolean putMlMetaIndexTemplateCheck = new AtomicBoolean(false);
    final AtomicBoolean putStateIndexTemplateCheck = new AtomicBoolean(false);
    final AtomicBoolean putResultsIndexTemplateCheck = new AtomicBoolean(false);

    // Allows us in test mode to disable the delay of shard allocation, so that in tests we don't have to wait for
    // for at least a minute for shards to get allocated.
    private final TimeValue delayedNodeTimeOutSetting;

    public MachineLearningTemplateRegistry(Settings settings, ClusterService clusterService, Client client,
                                           ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.threadPool = threadPool;
        // Whether we are using native process is a good way to detect whether we are in dev / test mode:
        if (MachineLearning.AUTODETECT_PROCESS.get(settings)) {
            delayedNodeTimeOutSetting = UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(settings);
        } else {
            delayedNodeTimeOutSetting = TimeValue.timeValueNanos(0);
        }

        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {

            // wait until the gateway has recovered from disk,
            // otherwise we think may not have the index templates while they actually do exist
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false) {
                addTemplatesIfMissing(event.state());
            }
        }
    }

    /**
     * Puts the registered index templates if missing to the
     * cluster waiting until the templates have been updated.
     */
    public void addTemplatesIfMissing(ClusterState state) {
        MetaData metaData = state.metaData();
        addMlNotificationsIndexTemplate(metaData);
        addMlMetaIndexTemplate(metaData);
        addStateIndexTemplate(metaData);
        addResultsIndexTemplate(metaData);
    }

    static boolean templateIsPresentAndUpToDate(String templateName, MetaData metaData) {
        IndexTemplateMetaData templateMetaData = metaData.templates().get(templateName);
        if (templateMetaData == null) {
            return false;
        }

        return templateMetaData.version() != null && templateMetaData.version() >= Version.CURRENT.id;
    }

    private void addMlNotificationsIndexTemplate(MetaData metaData) {
        if (putMlNotificationsIndexTemplateCheck.compareAndSet(false, true)) {
            if (templateIsPresentAndUpToDate(Auditor.NOTIFICATIONS_INDEX, metaData) == false) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    putNotificationMessageIndexTemplate((result, error) -> {
                        putMlNotificationsIndexTemplateCheck.set(false);
                        if (result) {
                            logger.info("successfully created {} index template", Auditor.NOTIFICATIONS_INDEX);
                        } else {
                            logger.error(
                                    new ParameterizedMessage("not able to create {} index template", Auditor.NOTIFICATIONS_INDEX), error);
                        }
                    });
                });
            } else {
                putMlNotificationsIndexTemplateCheck.set(false);
            }
        }
    }

    private void addMlMetaIndexTemplate(MetaData metaData) {
        if (putMlMetaIndexTemplateCheck.compareAndSet(false, true)) {
            if (templateIsPresentAndUpToDate(AnomalyDetectorsIndex.ML_META_INDEX, metaData) == false) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    putMetaIndexTemplate((result, error) -> {
                        putMlMetaIndexTemplateCheck.set(false);
                        if (result) {
                            logger.info("successfully created {} index template", AnomalyDetectorsIndex.ML_META_INDEX);
                        } else {
                            logger.error(new ParameterizedMessage(
                                    "not able to create {} index template", AnomalyDetectorsIndex.ML_META_INDEX), error);
                        }
                    });
                });
            } else {
                putMlMetaIndexTemplateCheck.set(false);
            }
        }
    }

    private void addStateIndexTemplate(MetaData metaData) {
        String stateIndexName = AnomalyDetectorsIndex.jobStateIndexName();
        if (putStateIndexTemplateCheck.compareAndSet(false, true)) {
            if (templateIsPresentAndUpToDate(stateIndexName, metaData) == false) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    putJobStateIndexTemplate((result, error) -> {
                        putStateIndexTemplateCheck.set(false);
                        if (result) {
                            logger.info("successfully created {} index template", stateIndexName);
                        } else {
                            logger.error("not able to create " + stateIndexName + " index template", error);
                        }
                    });
                });
            } else {
                putStateIndexTemplateCheck.set(false);
            }
        }
    }

    private void addResultsIndexTemplate(MetaData metaData) {
        if (putResultsIndexTemplateCheck.compareAndSet(false, true)) {
            if (templateIsPresentAndUpToDate(AnomalyDetectorsIndex.jobResultsIndexPrefix(), metaData) == false) {
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    putJobResultsIndexTemplate((result, error) -> {
                        putResultsIndexTemplateCheck.set(false);
                        if (result) {
                            logger.info("successfully created {} index template", AnomalyDetectorsIndex.jobResultsIndexPrefix());
                        } else {
                            logger.error(
                                    new ParameterizedMessage("not able to create {} index template",
                                            AnomalyDetectorsIndex.jobResultsIndexPrefix()), error);
                        }
                    });
                });
            } else {
                putResultsIndexTemplateCheck.set(false);
            }
        }
    }

    /**
     * Index template for notifications
     */
    void putNotificationMessageIndexTemplate(BiConsumer<Boolean, Exception> listener) {
        try {
            PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(Auditor.NOTIFICATIONS_INDEX);
            templateRequest.patterns(Collections.singletonList(Auditor.NOTIFICATIONS_INDEX));
            templateRequest.settings(mlNotificationIndexSettings());
            templateRequest.mapping(AuditMessage.TYPE.getPreferredName(), ElasticsearchMappings.auditMessageMapping());
            templateRequest.version(Version.CURRENT.id);
            client.admin().indices().putTemplate(templateRequest,
                    ActionListener.wrap(r -> listener.accept(true, null), e -> listener.accept(false, e)));
        } catch (IOException e) {
            logger.warn("Error putting the template for the notification message index", e);
            listener.accept(false,
                    new ElasticsearchException("Error creating the template mappings for the notification message indices", e));
        }
    }

    /**
     * Index template for meta data
     */
    void putMetaIndexTemplate(BiConsumer<Boolean, Exception> listener) {
        PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(AnomalyDetectorsIndex.ML_META_INDEX);
        templateRequest.patterns(Collections.singletonList(AnomalyDetectorsIndex.ML_META_INDEX));
        templateRequest.settings(mlNotificationIndexSettings());
        templateRequest.version(Version.CURRENT.id);

        client.admin().indices().putTemplate(templateRequest,
                ActionListener.wrap(r -> listener.accept(true, null), e -> listener.accept(false, e)));
    }

    void putJobStateIndexTemplate(BiConsumer<Boolean, Exception> listener) {
        try {
            XContentBuilder categorizerStateMapping = ElasticsearchMappings.categorizerStateMapping();
            XContentBuilder quantilesMapping = ElasticsearchMappings.quantilesMapping();
            XContentBuilder modelStateMapping = ElasticsearchMappings.modelStateMapping();

            PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(AnomalyDetectorsIndex.jobStateIndexName());
            templateRequest.patterns(Collections.singletonList(AnomalyDetectorsIndex.jobStateIndexName()));
            templateRequest.settings(mlStateIndexSettings());
            templateRequest.mapping(CategorizerState.TYPE, categorizerStateMapping);
            templateRequest.mapping(Quantiles.TYPE.getPreferredName(), quantilesMapping);
            templateRequest.mapping(ModelState.TYPE.getPreferredName(), modelStateMapping);
            templateRequest.version(Version.CURRENT.id);

            client.admin().indices().putTemplate(templateRequest,
                    ActionListener.wrap(r -> listener.accept(true, null), e -> listener.accept(false, e)));
        } catch (IOException e) {
            logger.error("Error creating template mappings for the " + AnomalyDetectorsIndex.jobStateIndexName() + " index", e);
            listener.accept(false, new ElasticsearchException("Error creating template mappings for the " +
                                        AnomalyDetectorsIndex.jobStateIndexName() + " indices", e));
        }
    }

    void putJobResultsIndexTemplate(BiConsumer<Boolean, Exception> listener) {
        try {
            XContentBuilder resultsMapping = ElasticsearchMappings.resultsMapping();
            XContentBuilder categoryDefinitionMapping = ElasticsearchMappings.categoryDefinitionMapping();
            XContentBuilder dataCountsMapping = ElasticsearchMappings.dataCountsMapping();
            XContentBuilder modelSnapshotMapping = ElasticsearchMappings.modelSnapshotMapping();

            PutIndexTemplateRequest templateRequest = new PutIndexTemplateRequest(AnomalyDetectorsIndex.jobResultsIndexPrefix());
            templateRequest.patterns(Collections.singletonList(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*"));
            templateRequest.settings(mlResultsIndexSettings());
            templateRequest.mapping(Result.TYPE.getPreferredName(), resultsMapping);
            templateRequest.mapping(CategoryDefinition.TYPE.getPreferredName(), categoryDefinitionMapping);
            templateRequest.mapping(DataCounts.TYPE.getPreferredName(), dataCountsMapping);
            templateRequest.mapping(ModelSnapshot.TYPE.getPreferredName(), modelSnapshotMapping);
            templateRequest.version(Version.CURRENT.id);

            client.admin().indices().putTemplate(templateRequest,
                    ActionListener.wrap(r -> listener.accept(true, null), e -> listener.accept(false, e)));
        } catch (IOException e) {
            logger.error("Error creating template mappings for the " + AnomalyDetectorsIndex.jobResultsIndexPrefix() + " indices", e);
            listener.accept(false, new ElasticsearchException("Error creating template mappings for the "
                    + AnomalyDetectorsIndex.jobResultsIndexPrefix() + " index", e));
        }
    }

    /**
     * Build the index settings that we want to apply to results indexes.
     *
     * @return Builder initialised with the desired setting for the ML results indices.
     */
    Settings.Builder mlResultsIndexSettings() {
        return Settings.builder()
                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-2")
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting)
                // Sacrifice durability for performance: in the event of power
                // failure we can lose the last 5 seconds of changes, but it's
                // much faster
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), ASYNC)
                // We need to allow fields not mentioned in the mappings to
                // pick up default mappings and be used in queries
                .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), true)
                // set the default all search field
                .put(IndexSettings.DEFAULT_FIELD_SETTING.getKey(), ElasticsearchMappings.ALL_FIELD_VALUES);
    }

    /**
     * Settings for the notification messages index
     *
     * @return Builder initialised with the desired setting for the ML index.
     */
    Settings.Builder mlNotificationIndexSettings() {
        return Settings.builder()
                // Our indexes are small and one shard puts the
                // least possible burden on Elasticsearch
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-2")
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting)
                // We need to allow fields not mentioned in the mappings to
                // pick up default mappings and be used in queries
                .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), true);
    }

     /**
     * Settings for the state index
     *
     * @return Builder initialised with the desired setting for the ML index.
     */
    Settings.Builder mlStateIndexSettings() {
        // TODO review these settings
        return Settings.builder()
                .put(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS, "0-2")
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delayedNodeTimeOutSetting)
                // Sacrifice durability for performance: in the event of power
                // failure we can lose the last 5 seconds of changes, but it's
                // much faster
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), ASYNC);
    }

    public static boolean allTemplatesInstalled(MetaData metaData) {
        boolean allPresent = true;
        for (String templateName : TEMPLATE_NAMES) {
            allPresent = allPresent && templateIsPresentAndUpToDate(templateName, metaData);
        }

        return allPresent;
    }
}
