/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.ml.action.GetJobsAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutFilterAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.ml.action.UpdateModelSnapshotAction;

public class MachineLearningClient {

    private final ElasticsearchClient client;

    public MachineLearningClient(ElasticsearchClient client) {
        this.client = client;
    }

    public void closeJob(CloseJobAction.Request request, ActionListener<CloseJobAction.Response> listener) {
        client.execute(CloseJobAction.INSTANCE, request, listener);
    }

    public void deleteDatafeed(DeleteDatafeedAction.Request request, ActionListener<DeleteDatafeedAction.Response> listener) {
        client.execute(DeleteDatafeedAction.INSTANCE, request, listener);
    }

    public void deleteFilter(DeleteFilterAction.Request request, ActionListener<DeleteFilterAction.Response> listener) {
        client.execute(DeleteFilterAction.INSTANCE, request, listener);
    }

    public void deleteJob(DeleteJobAction.Request request, ActionListener<DeleteJobAction.Response> listener) {
        client.execute(DeleteJobAction.INSTANCE, request, listener);
    }

    public void deleteModelSnapshot(DeleteModelSnapshotAction.Request request,
            ActionListener<DeleteModelSnapshotAction.Response> listener) {
        client.execute(DeleteModelSnapshotAction.INSTANCE, request, listener);
    }

    public void flushJob(FlushJobAction.Request request, ActionListener<FlushJobAction.Response> listener) {
        client.execute(FlushJobAction.INSTANCE, request, listener);
    }

    public void getBuckets(GetBucketsAction.Request request, ActionListener<GetBucketsAction.Response> listener) {
        client.execute(GetBucketsAction.INSTANCE, request, listener);
    }

    public void getCategories(GetCategoriesAction.Request request, ActionListener<GetCategoriesAction.Response> listener) {
        client.execute(GetCategoriesAction.INSTANCE, request, listener);
    }

    public void getDatafeeds(GetDatafeedsAction.Request request, ActionListener<GetDatafeedsAction.Response> listener) {
        client.execute(GetDatafeedsAction.INSTANCE, request, listener);
    }

    public void getDatafeedsStats(GetDatafeedsStatsAction.Request request, ActionListener<GetDatafeedsStatsAction.Response> listener) {
        client.execute(GetDatafeedsStatsAction.INSTANCE, request, listener);
    }

    public void getFilters(GetFiltersAction.Request request, ActionListener<GetFiltersAction.Response> listener) {
        client.execute(GetFiltersAction.INSTANCE, request, listener);
    }

    public void getInfluencers(GetInfluencersAction.Request request, ActionListener<GetInfluencersAction.Response> listener) {
        client.execute(GetInfluencersAction.INSTANCE, request, listener);
    }

    public void getJobs(GetJobsAction.Request request, ActionListener<GetJobsAction.Response> listener) {
        client.execute(GetJobsAction.INSTANCE, request, listener);
    }

    public void getJobsStats(GetJobsStatsAction.Request request, ActionListener<GetJobsStatsAction.Response> listener) {
        client.execute(GetJobsStatsAction.INSTANCE, request, listener);
    }

    public void getModelSnapshots(GetModelSnapshotsAction.Request request, ActionListener<GetModelSnapshotsAction.Response> listener) {
        client.execute(GetModelSnapshotsAction.INSTANCE, request, listener);
    }

    public void getRecords(GetRecordsAction.Request request, ActionListener<GetRecordsAction.Response> listener) {
        client.execute(GetRecordsAction.INSTANCE, request, listener);
    }

    public void openJob(OpenJobAction.Request request, ActionListener<OpenJobAction.Response> listener) {
        client.execute(OpenJobAction.INSTANCE, request, listener);
    }

    public void postData(PostDataAction.Request request, ActionListener<PostDataAction.Response> listener) {
        client.execute(PostDataAction.INSTANCE, request, listener);
    }

    public void putDatafeed(PutDatafeedAction.Request request, ActionListener<PutDatafeedAction.Response> listener) {
        client.execute(PutDatafeedAction.INSTANCE, request, listener);
    }

    public void putFilter(PutFilterAction.Request request, ActionListener<PutFilterAction.Response> listener) {
        client.execute(PutFilterAction.INSTANCE, request, listener);
    }

    public void putJob(PutJobAction.Request request, ActionListener<PutJobAction.Response> listener) {
        client.execute(PutJobAction.INSTANCE, request, listener);
    }

    public void revertModelSnapshot(RevertModelSnapshotAction.Request request,
            ActionListener<RevertModelSnapshotAction.Response> listener) {
        client.execute(RevertModelSnapshotAction.INSTANCE, request, listener);
    }

    public void startDatafeed(StartDatafeedAction.Request request, ActionListener<StartDatafeedAction.Response> listener) {
        client.execute(StartDatafeedAction.INSTANCE, request, listener);
    }

    public void stopDatafeed(StopDatafeedAction.Request request, ActionListener<StopDatafeedAction.Response> listener) {
        client.execute(StopDatafeedAction.INSTANCE, request, listener);
    }

    public void updateDatafeed(UpdateDatafeedAction.Request request, ActionListener<PutDatafeedAction.Response> listener) {
        client.execute(UpdateDatafeedAction.INSTANCE, request, listener);
    }

    public void updateJob(UpdateJobAction.Request request, ActionListener<PutJobAction.Response> listener) {
        client.execute(UpdateJobAction.INSTANCE, request, listener);
    }

    public void updateModelSnapshot(UpdateModelSnapshotAction.Request request,
            ActionListener<UpdateModelSnapshotAction.Response> listener) {
        client.execute(UpdateModelSnapshotAction.INSTANCE, request, listener);
    }
}
