/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.DeleteDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteTrainedModelRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.GetDatafeedRequest;
import org.elasticsearch.client.ml.GetDatafeedResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetTrainedModelsRequest;
import org.elasticsearch.client.ml.GetTrainedModelsStatsRequest;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.client.ml.job.config.Job;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Cleans up and ML resources created during tests
 */
public class MlTestStateCleaner {

    private static final Set<String> NOT_DELETED_TRAINED_MODELS = Collections.singleton("lang_ident_model_1");
    private final Logger logger;
    private final MachineLearningClient mlClient;
    private final RestHighLevelClient client;

    public MlTestStateCleaner(Logger logger, RestHighLevelClient client) {
        this.logger = logger;
        this.mlClient = client.machineLearning();
        this.client = client;
    }

    public void clearMlMetadata() throws IOException {
        deleteAllTrainedModels();
        deleteAllDatafeeds();
        deleteAllJobs();
        deleteAllDataFrameAnalytics();
    }

    @SuppressWarnings("unchecked")
    private void deleteAllTrainedModels() throws IOException {
        Set<String> pipelinesWithModels = mlClient.getTrainedModelsStats(
            new GetTrainedModelsStatsRequest("_all").setPageParams(new PageParams(0, 10_000)), RequestOptions.DEFAULT
        ).getTrainedModelStats()
            .stream()
            .flatMap(stats -> {
                Map<String, Object> ingestStats = stats.getIngestStats();
                if (ingestStats == null || ingestStats.isEmpty()) {
                    return Stream.empty();
                }
                Map<String, Object> pipelines = (Map<String, Object>)ingestStats.get("pipelines");
                if (pipelines == null || pipelines.isEmpty()) {
                    return Stream.empty();
                }
                return pipelines.keySet().stream();
            })
            .collect(Collectors.toSet());
        for (String pipelineId : pipelinesWithModels) {
            try {
                client.ingest().deletePipeline(new DeletePipelineRequest(pipelineId), RequestOptions.DEFAULT);
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("failed to delete pipeline [{}]", pipelineId), ex);
            }
        }

        mlClient.getTrainedModels(
            GetTrainedModelsRequest.getAllTrainedModelConfigsRequest().setPageParams(new PageParams(0, 10_000)),
            RequestOptions.DEFAULT)
            .getTrainedModels()
            .stream()
            .filter(trainedModelConfig -> NOT_DELETED_TRAINED_MODELS.contains(trainedModelConfig.getModelId()) == false)
            .forEach(config -> {
                try {
                    mlClient.deleteTrainedModel(new DeleteTrainedModelRequest(config.getModelId()), RequestOptions.DEFAULT);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            });
    }

    private void deleteAllDatafeeds() throws IOException {
        stopAllDatafeeds();

        GetDatafeedResponse getDatafeedResponse = mlClient.getDatafeed(GetDatafeedRequest.getAllDatafeedsRequest(), RequestOptions.DEFAULT);
        for (DatafeedConfig datafeed : getDatafeedResponse.datafeeds()) {
            mlClient.deleteDatafeed(new DeleteDatafeedRequest(datafeed.getId()), RequestOptions.DEFAULT);
        }
    }

    private void stopAllDatafeeds() {
        StopDatafeedRequest stopAllDatafeedsRequest = StopDatafeedRequest.stopAllDatafeedsRequest();
        try {
            mlClient.stopDatafeed(stopAllDatafeedsRequest, RequestOptions.DEFAULT);
        } catch (Exception e1) {
            logger.warn("failed to stop all datafeeds. Forcing stop", e1);
            try {
                stopAllDatafeedsRequest.setForce(true);
                mlClient.stopDatafeed(stopAllDatafeedsRequest, RequestOptions.DEFAULT);
            } catch (Exception e2) {
                logger.warn("Force-closing all data feeds failed", e2);
            }
            throw new RuntimeException("Had to resort to force-stopping datafeeds, something went wrong?", e1);
        }
    }

    private void deleteAllJobs() throws IOException {
        closeAllJobs();

        GetJobResponse getJobResponse = mlClient.getJob(GetJobRequest.getAllJobsRequest(), RequestOptions.DEFAULT);
        for (Job job : getJobResponse.jobs()) {
            mlClient.deleteJob(new DeleteJobRequest(job.getId()), RequestOptions.DEFAULT);
        }
    }

    private void closeAllJobs() {
        CloseJobRequest closeAllJobsRequest = CloseJobRequest.closeAllJobsRequest();
        try {
            mlClient.closeJob(closeAllJobsRequest, RequestOptions.DEFAULT);
        } catch (Exception e1) {
            logger.warn("failed to close all jobs. Forcing closed", e1);
            closeAllJobsRequest.setForce(true);
            try {
                mlClient.closeJob(closeAllJobsRequest, RequestOptions.DEFAULT);
            } catch (Exception e2) {
                logger.warn("Force-closing all jobs failed", e2);
            }
            throw new RuntimeException("Had to resort to force-closing jobs, something went wrong?", e1);
        }
    }

    private void deleteAllDataFrameAnalytics() throws IOException {
        stopAllDataFrameAnalytics();

        GetDataFrameAnalyticsResponse getDataFrameAnalyticsResponse =
            mlClient.getDataFrameAnalytics(GetDataFrameAnalyticsRequest.getAllDataFrameAnalyticsRequest(), RequestOptions.DEFAULT);
        for (DataFrameAnalyticsConfig config : getDataFrameAnalyticsResponse.getAnalytics()) {
            mlClient.deleteDataFrameAnalytics(new DeleteDataFrameAnalyticsRequest(config.getId()), RequestOptions.DEFAULT);
        }
    }

    private void stopAllDataFrameAnalytics() {
        StopDataFrameAnalyticsRequest stopAllRequest = new StopDataFrameAnalyticsRequest("*");
        try {
            mlClient.stopDataFrameAnalytics(stopAllRequest, RequestOptions.DEFAULT);
        } catch (Exception e1) {
            logger.warn("failed to stop all data frame analytics. Will proceed to force-stopping", e1);
            stopAllRequest.setForce(true);
            try {
                mlClient.stopDataFrameAnalytics(stopAllRequest, RequestOptions.DEFAULT);
            } catch (Exception e2) {
                logger.warn("force-stopping all data frame analytics failed", e2);
            }
            throw new RuntimeException("Had to resort to force-stopping data frame analytics, something went wrong?", e1);
        }
    }
}
