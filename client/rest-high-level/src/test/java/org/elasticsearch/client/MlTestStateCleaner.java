/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.DeleteDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.GetDatafeedRequest;
import org.elasticsearch.client.ml.GetDatafeedResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.client.ml.job.config.Job;

import java.io.IOException;

/**
 * Cleans up and ML resources created during tests
 */
public class MlTestStateCleaner {

    private final Logger logger;
    private final MachineLearningClient mlClient;

    public MlTestStateCleaner(Logger logger, MachineLearningClient mlClient) {
        this.logger = logger;
        this.mlClient = mlClient;
    }

    public void clearMlMetadata() throws IOException {
        deleteAllDatafeeds();
        deleteAllJobs();
        deleteAllDataFrameAnalytics();
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
