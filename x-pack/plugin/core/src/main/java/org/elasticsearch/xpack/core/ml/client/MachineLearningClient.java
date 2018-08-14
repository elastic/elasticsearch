/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.ValidateDetectorAction;
import org.elasticsearch.xpack.core.ml.action.ValidateJobConfigAction;

public class MachineLearningClient {

    private final ElasticsearchClient client;

    public MachineLearningClient(ElasticsearchClient client) {
        this.client = client;
    }

    public void closeJob(CloseJobAction.Request request,
            ActionListener<CloseJobAction.Response> listener) {
        client.execute(CloseJobAction.INSTANCE, request, listener);
    }

    public ActionFuture<CloseJobAction.Response> closeJob(CloseJobAction.Request request) {
        PlainActionFuture<CloseJobAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(CloseJobAction.INSTANCE, request, listener);
        return listener;
    }

    public void deleteDatafeed(DeleteDatafeedAction.Request request,
            ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteDatafeedAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deleteDatafeed(
            DeleteDatafeedAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(DeleteDatafeedAction.INSTANCE, request, listener);
        return listener;
    }

    public void deleteFilter(DeleteFilterAction.Request request,
            ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteFilterAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deleteFilter(
            DeleteFilterAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(DeleteFilterAction.INSTANCE, request, listener);
        return listener;
    }

    public void deleteJob(DeleteJobAction.Request request,
            ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteJobAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deleteJob(DeleteJobAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(DeleteJobAction.INSTANCE, request, listener);
        return listener;
    }

    public void deleteModelSnapshot(DeleteModelSnapshotAction.Request request,
            ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteModelSnapshotAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deleteModelSnapshot(
            DeleteModelSnapshotAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(DeleteModelSnapshotAction.INSTANCE, request, listener);
        return listener;
    }

    public void flushJob(FlushJobAction.Request request,
            ActionListener<FlushJobAction.Response> listener) {
        client.execute(FlushJobAction.INSTANCE, request, listener);
    }

    public ActionFuture<FlushJobAction.Response> flushJob(FlushJobAction.Request request) {
        PlainActionFuture<FlushJobAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(FlushJobAction.INSTANCE, request, listener);
        return listener;
    }

    public void getBuckets(GetBucketsAction.Request request,
            ActionListener<GetBucketsAction.Response> listener) {
        client.execute(GetBucketsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetBucketsAction.Response> getBuckets(GetBucketsAction.Request request) {
        PlainActionFuture<GetBucketsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetBucketsAction.INSTANCE, request, listener);
        return listener;
    }

    public void getCategories(GetCategoriesAction.Request request,
            ActionListener<GetCategoriesAction.Response> listener) {
        client.execute(GetCategoriesAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetCategoriesAction.Response> getCategories(
            GetCategoriesAction.Request request) {
        PlainActionFuture<GetCategoriesAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetCategoriesAction.INSTANCE, request, listener);
        return listener;
    }

    public void getDatafeeds(GetDatafeedsAction.Request request,
            ActionListener<GetDatafeedsAction.Response> listener) {
        client.execute(GetDatafeedsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetDatafeedsAction.Response> getDatafeeds(
            GetDatafeedsAction.Request request) {
        PlainActionFuture<GetDatafeedsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetDatafeedsAction.INSTANCE, request, listener);
        return listener;
    }

    public void getDatafeedsStats(GetDatafeedsStatsAction.Request request,
            ActionListener<GetDatafeedsStatsAction.Response> listener) {
        client.execute(GetDatafeedsStatsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetDatafeedsStatsAction.Response> getDatafeedsStats(
            GetDatafeedsStatsAction.Request request) {
        PlainActionFuture<GetDatafeedsStatsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetDatafeedsStatsAction.INSTANCE, request, listener);
        return listener;
    }

    public void getFilters(GetFiltersAction.Request request,
            ActionListener<GetFiltersAction.Response> listener) {
        client.execute(GetFiltersAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetFiltersAction.Response> getFilters(GetFiltersAction.Request request) {
        PlainActionFuture<GetFiltersAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetFiltersAction.INSTANCE, request, listener);
        return listener;
    }

    public void getInfluencers(GetInfluencersAction.Request request,
                               ActionListener<GetInfluencersAction.Response> listener) {
        client.execute(GetInfluencersAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetInfluencersAction.Response> getInfluencers(
            GetInfluencersAction.Request request) {
        PlainActionFuture<GetInfluencersAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetInfluencersAction.INSTANCE, request, listener);
        return listener;
    }

    public void getJobs(GetJobsAction.Request request,
            ActionListener<GetJobsAction.Response> listener) {
        client.execute(GetJobsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetJobsAction.Response> getJobs(GetJobsAction.Request request) {
        PlainActionFuture<GetJobsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetJobsAction.INSTANCE, request, listener);
        return listener;
    }

    public void getJobsStats(GetJobsStatsAction.Request request,
            ActionListener<GetJobsStatsAction.Response> listener) {
        client.execute(GetJobsStatsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetJobsStatsAction.Response> getJobsStats(
            GetJobsStatsAction.Request request) {
        PlainActionFuture<GetJobsStatsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetJobsStatsAction.INSTANCE, request, listener);
        return listener;
    }

    public void getModelSnapshots(GetModelSnapshotsAction.Request request,
            ActionListener<GetModelSnapshotsAction.Response> listener) {
        client.execute(GetModelSnapshotsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetModelSnapshotsAction.Response> getModelSnapshots(
            GetModelSnapshotsAction.Request request) {
        PlainActionFuture<GetModelSnapshotsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetModelSnapshotsAction.INSTANCE, request, listener);
        return listener;
    }

    public void getRecords(GetRecordsAction.Request request,
            ActionListener<GetRecordsAction.Response> listener) {
        client.execute(GetRecordsAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetRecordsAction.Response> getRecords(GetRecordsAction.Request request) {
        PlainActionFuture<GetRecordsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetRecordsAction.INSTANCE, request, listener);
        return listener;
    }

    public void openJob(OpenJobAction.Request request,
            ActionListener<AcknowledgedResponse> listener) {
        client.execute(OpenJobAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> openJob(OpenJobAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(OpenJobAction.INSTANCE, request, listener);
        return listener;
    }

    public void postData(PostDataAction.Request request,
            ActionListener<PostDataAction.Response> listener) {
        client.execute(PostDataAction.INSTANCE, request, listener);
    }

    public ActionFuture<PostDataAction.Response> postData(PostDataAction.Request request) {
        PlainActionFuture<PostDataAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(PostDataAction.INSTANCE, request, listener);
        return listener;
    }

    public void putDatafeed(PutDatafeedAction.Request request,
            ActionListener<PutDatafeedAction.Response> listener) {
        client.execute(PutDatafeedAction.INSTANCE, request, listener);
    }

    public ActionFuture<PutDatafeedAction.Response> putDatafeed(PutDatafeedAction.Request request) {
        PlainActionFuture<PutDatafeedAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(PutDatafeedAction.INSTANCE, request, listener);
        return listener;
    }

    public void putFilter(PutFilterAction.Request request,
            ActionListener<PutFilterAction.Response> listener) {
        client.execute(PutFilterAction.INSTANCE, request, listener);
    }

    public ActionFuture<PutFilterAction.Response> putFilter(PutFilterAction.Request request) {
        PlainActionFuture<PutFilterAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(PutFilterAction.INSTANCE, request, listener);
        return listener;
    }

    public void putJob(PutJobAction.Request request,
            ActionListener<PutJobAction.Response> listener) {
        client.execute(PutJobAction.INSTANCE, request, listener);
    }

    public ActionFuture<PutJobAction.Response> putJob(PutJobAction.Request request) {
        PlainActionFuture<PutJobAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(PutJobAction.INSTANCE, request, listener);
        return listener;
    }

    public void revertModelSnapshot(RevertModelSnapshotAction.Request request,
            ActionListener<RevertModelSnapshotAction.Response> listener) {
        client.execute(RevertModelSnapshotAction.INSTANCE, request, listener);
    }

    public ActionFuture<RevertModelSnapshotAction.Response> revertModelSnapshot(
            RevertModelSnapshotAction.Request request) {
        PlainActionFuture<RevertModelSnapshotAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(RevertModelSnapshotAction.INSTANCE, request, listener);
        return listener;
    }

    public void startDatafeed(StartDatafeedAction.Request request,
            ActionListener<AcknowledgedResponse> listener) {
        client.execute(StartDatafeedAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> startDatafeed(
            StartDatafeedAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(StartDatafeedAction.INSTANCE, request, listener);
        return listener;
    }

    public void stopDatafeed(StopDatafeedAction.Request request,
            ActionListener<StopDatafeedAction.Response> listener) {
        client.execute(StopDatafeedAction.INSTANCE, request, listener);
    }

    public ActionFuture<StopDatafeedAction.Response> stopDatafeed(
            StopDatafeedAction.Request request) {
        PlainActionFuture<StopDatafeedAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(StopDatafeedAction.INSTANCE, request, listener);
        return listener;
    }

    public void updateDatafeed(UpdateDatafeedAction.Request request,
            ActionListener<PutDatafeedAction.Response> listener) {
        client.execute(UpdateDatafeedAction.INSTANCE, request, listener);
    }

    public ActionFuture<PutDatafeedAction.Response> updateDatafeed(
            UpdateDatafeedAction.Request request) {
        PlainActionFuture<PutDatafeedAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(UpdateDatafeedAction.INSTANCE, request, listener);
        return listener;
    }

    public void updateJob(UpdateJobAction.Request request,
            ActionListener<PutJobAction.Response> listener) {
        client.execute(UpdateJobAction.INSTANCE, request, listener);
    }

    public ActionFuture<PutJobAction.Response> updateJob(UpdateJobAction.Request request) {
        PlainActionFuture<PutJobAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(UpdateJobAction.INSTANCE, request, listener);
        return listener;
    }

    public void updateModelSnapshot(UpdateModelSnapshotAction.Request request,
            ActionListener<UpdateModelSnapshotAction.Response> listener) {
        client.execute(UpdateModelSnapshotAction.INSTANCE, request, listener);
    }

    public ActionFuture<UpdateModelSnapshotAction.Response> updateModelSnapshot(
            UpdateModelSnapshotAction.Request request) {
        PlainActionFuture<UpdateModelSnapshotAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(UpdateModelSnapshotAction.INSTANCE, request, listener);
        return listener;
    }

    public void validateDetector(ValidateDetectorAction.Request request,
                                    ActionListener<AcknowledgedResponse> listener) {
        client.execute(ValidateDetectorAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> validateDetector(
            ValidateDetectorAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(ValidateDetectorAction.INSTANCE, request, listener);
        return listener;
    }

    public void validateJobConfig(ValidateJobConfigAction.Request request,
                                 ActionListener<AcknowledgedResponse> listener) {
        client.execute(ValidateJobConfigAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> validateJobConfig(
            ValidateJobConfigAction.Request request) {
        PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(ValidateJobConfigAction.INSTANCE, request, listener);
        return listener;
    }
}
