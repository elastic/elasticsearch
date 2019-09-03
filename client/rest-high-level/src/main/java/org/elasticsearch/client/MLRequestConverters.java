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

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.RequestConverters.EndpointBuilder;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.DeleteCalendarEventRequest;
import org.elasticsearch.client.ml.DeleteCalendarJobRequest;
import org.elasticsearch.client.ml.DeleteCalendarRequest;
import org.elasticsearch.client.ml.DeleteDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataRequest;
import org.elasticsearch.client.ml.DeleteFilterRequest;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteModelSnapshotRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.client.ml.FindFileStructureRequest;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetCalendarEventsRequest;
import org.elasticsearch.client.ml.GetCalendarsRequest;
import org.elasticsearch.client.ml.GetCategoriesRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsStatsRequest;
import org.elasticsearch.client.ml.GetDatafeedRequest;
import org.elasticsearch.client.ml.GetDatafeedStatsRequest;
import org.elasticsearch.client.ml.GetFiltersRequest;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.MlInfoRequest;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.PostCalendarEventRequest;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PreviewDatafeedRequest;
import org.elasticsearch.client.ml.PutCalendarJobRequest;
import org.elasticsearch.client.ml.PutCalendarRequest;
import org.elasticsearch.client.ml.PutDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutFilterRequest;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.RevertModelSnapshotRequest;
import org.elasticsearch.client.ml.SetUpgradeModeRequest;
import org.elasticsearch.client.ml.StartDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StartDatafeedRequest;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.UpdateDatafeedRequest;
import org.elasticsearch.client.ml.UpdateFilterRequest;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.UpdateModelSnapshotRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createContentType;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class MLRequestConverters {

    private MLRequestConverters() {}

    static Request putJob(PutJobRequest putJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(putJobRequest.getJob().getId())
                .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getJob(GetJobRequest getJobRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(Strings.collectionToCommaDelimitedString(getJobRequest.getJobIds()))
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (getJobRequest.getAllowNoJobs() != null) {
            params.putParam("allow_no_jobs", Boolean.toString(getJobRequest.getAllowNoJobs()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request getJobStats(GetJobStatsRequest getJobStatsRequest) {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(Strings.collectionToCommaDelimitedString(getJobStatsRequest.getJobIds()))
                .addPathPartAsIs("_stats")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (getJobStatsRequest.getAllowNoJobs() != null) {
            params.putParam("allow_no_jobs", Boolean.toString(getJobStatsRequest.getAllowNoJobs()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request openJob(OpenJobRequest openJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(openJobRequest.getJobId())
                .addPathPartAsIs("_open")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(openJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request closeJob(CloseJobRequest closeJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(Strings.collectionToCommaDelimitedString(closeJobRequest.getJobIds()))
            .addPathPartAsIs("_close")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(closeJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteExpiredData(DeleteExpiredDataRequest deleteExpiredDataRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("_delete_expired_data")
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        return request;
    }

    static Request deleteJob(DeleteJobRequest deleteJobRequest) {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(deleteJobRequest.getJobId())
                .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (deleteJobRequest.getForce() != null) {
            params.putParam("force", Boolean.toString(deleteJobRequest.getForce()));
        }
        if (deleteJobRequest.getWaitForCompletion() != null) {
            params.putParam("wait_for_completion", Boolean.toString(deleteJobRequest.getWaitForCompletion()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request flushJob(FlushJobRequest flushJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(flushJobRequest.getJobId())
                .addPathPartAsIs("_flush")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(flushJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request forecastJob(ForecastJobRequest forecastJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(forecastJobRequest.getJobId())
            .addPathPartAsIs("_forecast")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(forecastJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request updateJob(UpdateJobRequest updateJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(updateJobRequest.getJobUpdate().getJobId())
                .addPathPartAsIs("_update")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(updateJobRequest.getJobUpdate(), REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request putDatafeed(PutDatafeedRequest putDatafeedRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("datafeeds")
                .addPathPart(putDatafeedRequest.getDatafeed().getId())
                .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putDatafeedRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request updateDatafeed(UpdateDatafeedRequest updateDatafeedRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("datafeeds")
            .addPathPart(updateDatafeedRequest.getDatafeedUpdate().getId())
            .addPathPartAsIs("_update")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(updateDatafeedRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getDatafeed(GetDatafeedRequest getDatafeedRequest) {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("datafeeds")
                .addPathPart(Strings.collectionToCommaDelimitedString(getDatafeedRequest.getDatafeedIds()))
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (getDatafeedRequest.getAllowNoDatafeeds() != null) {
            params.putParam(GetDatafeedRequest.ALLOW_NO_DATAFEEDS.getPreferredName(),
                    Boolean.toString(getDatafeedRequest.getAllowNoDatafeeds()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteDatafeed(DeleteDatafeedRequest deleteDatafeedRequest) {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("datafeeds")
                .addPathPart(deleteDatafeedRequest.getDatafeedId())
                .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (deleteDatafeedRequest.getForce() != null) {
            params.putParam("force", Boolean.toString(deleteDatafeedRequest.getForce()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request startDatafeed(StartDatafeedRequest startDatafeedRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("datafeeds")
            .addPathPart(startDatafeedRequest.getDatafeedId())
            .addPathPartAsIs("_start")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(startDatafeedRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request stopDatafeed(StopDatafeedRequest stopDatafeedRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("datafeeds")
            .addPathPart(Strings.collectionToCommaDelimitedString(stopDatafeedRequest.getDatafeedIds()))
            .addPathPartAsIs("_stop")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(stopDatafeedRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getDatafeedStats(GetDatafeedStatsRequest getDatafeedStatsRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("datafeeds")
            .addPathPart(Strings.collectionToCommaDelimitedString(getDatafeedStatsRequest.getDatafeedIds()))
            .addPathPartAsIs("_stats")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (getDatafeedStatsRequest.getAllowNoDatafeeds() != null) {
            params.putParam("allow_no_datafeeds", Boolean.toString(getDatafeedStatsRequest.getAllowNoDatafeeds()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request previewDatafeed(PreviewDatafeedRequest previewDatafeedRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("datafeeds")
            .addPathPart(previewDatafeedRequest.getDatafeedId())
            .addPathPartAsIs("_preview")
            .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    static Request deleteForecast(DeleteForecastRequest deleteForecastRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(deleteForecastRequest.getJobId())
            .addPathPartAsIs("_forecast")
            .addPathPart(Strings.collectionToCommaDelimitedString(deleteForecastRequest.getForecastIds()))
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (deleteForecastRequest.getAllowNoForecasts() != null) {
            params.putParam("allow_no_forecasts", Boolean.toString(deleteForecastRequest.getAllowNoForecasts()));
        }
        if (deleteForecastRequest.timeout() != null) {
            params.putParam("timeout", deleteForecastRequest.timeout().getStringRep());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteModelSnapshot(DeleteModelSnapshotRequest deleteModelSnapshotRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(deleteModelSnapshotRequest.getJobId())
            .addPathPartAsIs("model_snapshots")
            .addPathPart(deleteModelSnapshotRequest.getSnapshotId())
            .build();
        return new Request(HttpDelete.METHOD_NAME, endpoint);
    }

    static Request getBuckets(GetBucketsRequest getBucketsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(getBucketsRequest.getJobId())
                .addPathPartAsIs("results")
                .addPathPartAsIs("buckets")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getBucketsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getCategories(GetCategoriesRequest getCategoriesRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(getCategoriesRequest.getJobId())
            .addPathPartAsIs("results")
            .addPathPartAsIs("categories")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getCategoriesRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getModelSnapshots(GetModelSnapshotsRequest getModelSnapshotsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(getModelSnapshotsRequest.getJobId())
            .addPathPartAsIs("model_snapshots")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getModelSnapshotsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request updateModelSnapshot(UpdateModelSnapshotRequest updateModelSnapshotRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(updateModelSnapshotRequest.getJobId())
            .addPathPartAsIs("model_snapshots")
            .addPathPart(updateModelSnapshotRequest.getSnapshotId())
            .addPathPartAsIs("_update")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(updateModelSnapshotRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request revertModelSnapshot(RevertModelSnapshotRequest revertModelSnapshotsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(revertModelSnapshotsRequest.getJobId())
            .addPathPartAsIs("model_snapshots")
            .addPathPart(revertModelSnapshotsRequest.getSnapshotId())
            .addPathPart("_revert")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(revertModelSnapshotsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getOverallBuckets(GetOverallBucketsRequest getOverallBucketsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(Strings.collectionToCommaDelimitedString(getOverallBucketsRequest.getJobIds()))
                .addPathPartAsIs("results")
                .addPathPartAsIs("overall_buckets")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getOverallBucketsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getRecords(GetRecordsRequest getRecordsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(getRecordsRequest.getJobId())
                .addPathPartAsIs("results")
                .addPathPartAsIs("records")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getRecordsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request postData(PostDataRequest postDataRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(postDataRequest.getJobId())
            .addPathPartAsIs("_data")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (postDataRequest.getResetStart() != null) {
            params.putParam(PostDataRequest.RESET_START.getPreferredName(), postDataRequest.getResetStart());
        }
        if (postDataRequest.getResetEnd() != null) {
            params.putParam(PostDataRequest.RESET_END.getPreferredName(), postDataRequest.getResetEnd());
        }
        BytesReference content = postDataRequest.getContent();
        request.addParameters(params.asMap());
        if (content != null) {
            BytesRef source = postDataRequest.getContent().toBytesRef();
            HttpEntity byteEntity = new NByteArrayEntity(source.bytes,
                source.offset,
                source.length,
                createContentType(postDataRequest.getXContentType()));
            request.setEntity(byteEntity);
        }
        return request;
    }

    static Request getInfluencers(GetInfluencersRequest getInfluencersRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(getInfluencersRequest.getJobId())
                .addPathPartAsIs("results")
                .addPathPartAsIs("influencers")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getInfluencersRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request putCalendar(PutCalendarRequest putCalendarRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("calendars")
                .addPathPart(putCalendarRequest.getCalendar().getId())
                .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putCalendarRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getCalendars(GetCalendarsRequest getCalendarsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("calendars")
                .addPathPart(getCalendarsRequest.getCalendarId())
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getCalendarsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request putCalendarJob(PutCalendarJobRequest putCalendarJobRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("calendars")
            .addPathPart(putCalendarJobRequest.getCalendarId())
            .addPathPartAsIs("jobs")
            .addPathPart(Strings.collectionToCommaDelimitedString(putCalendarJobRequest.getJobIds()))
            .build();
        return new Request(HttpPut.METHOD_NAME, endpoint);
    }

    static Request deleteCalendarJob(DeleteCalendarJobRequest deleteCalendarJobRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("calendars")
            .addPathPart(deleteCalendarJobRequest.getCalendarId())
            .addPathPartAsIs("jobs")
            .addPathPart(Strings.collectionToCommaDelimitedString(deleteCalendarJobRequest.getJobIds()))
            .build();
        return new Request(HttpDelete.METHOD_NAME, endpoint);
    }

    static Request deleteCalendar(DeleteCalendarRequest deleteCalendarRequest) {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_ml")
                .addPathPartAsIs("calendars")
                .addPathPart(deleteCalendarRequest.getCalendarId())
                .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        return request;
    }

    static Request getCalendarEvents(GetCalendarEventsRequest getCalendarEventsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("calendars")
            .addPathPart(getCalendarEventsRequest.getCalendarId())
            .addPathPartAsIs("events")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getCalendarEventsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request postCalendarEvents(PostCalendarEventRequest postCalendarEventRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("calendars")
            .addPathPart(postCalendarEventRequest.getCalendarId())
            .addPathPartAsIs("events")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(postCalendarEventRequest,
            REQUEST_BODY_CONTENT_TYPE,
            PostCalendarEventRequest.EXCLUDE_CALENDAR_ID_PARAMS));
        return request;
    }

    static Request deleteCalendarEvent(DeleteCalendarEventRequest deleteCalendarEventRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("calendars")
            .addPathPart(deleteCalendarEventRequest.getCalendarId())
            .addPathPartAsIs("events")
            .addPathPart(deleteCalendarEventRequest.getEventId())
            .build();
        return new Request(HttpDelete.METHOD_NAME, endpoint);
    }

    static Request putDataFrameAnalytics(PutDataFrameAnalyticsRequest putRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "analytics")
            .addPathPart(putRequest.getConfig().getId())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getDataFrameAnalytics(GetDataFrameAnalyticsRequest getRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "analytics")
            .addPathPart(Strings.collectionToCommaDelimitedString(getRequest.getIds()))
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (getRequest.getPageParams() != null) {
            PageParams pageParams = getRequest.getPageParams();
            if (pageParams.getFrom() != null) {
                params.putParam(PageParams.FROM.getPreferredName(), pageParams.getFrom().toString());
            }
            if (pageParams.getSize() != null) {
                params.putParam(PageParams.SIZE.getPreferredName(), pageParams.getSize().toString());
            }
        }
        if (getRequest.getAllowNoMatch() != null) {
            params.putParam(GetDataFrameAnalyticsRequest.ALLOW_NO_MATCH.getPreferredName(), Boolean.toString(getRequest.getAllowNoMatch()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request getDataFrameAnalyticsStats(GetDataFrameAnalyticsStatsRequest getStatsRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "analytics")
            .addPathPart(Strings.collectionToCommaDelimitedString(getStatsRequest.getIds()))
            .addPathPartAsIs("_stats")
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (getStatsRequest.getPageParams() != null) {
            PageParams pageParams = getStatsRequest.getPageParams();
            if (pageParams.getFrom() != null) {
                params.putParam(PageParams.FROM.getPreferredName(), pageParams.getFrom().toString());
            }
            if (pageParams.getSize() != null) {
                params.putParam(PageParams.SIZE.getPreferredName(), pageParams.getSize().toString());
            }
        }
        if (getStatsRequest.getAllowNoMatch() != null) {
            params.putParam(GetDataFrameAnalyticsStatsRequest.ALLOW_NO_MATCH.getPreferredName(),
                Boolean.toString(getStatsRequest.getAllowNoMatch()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request startDataFrameAnalytics(StartDataFrameAnalyticsRequest startRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "analytics")
            .addPathPart(startRequest.getId())
            .addPathPartAsIs("_start")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (startRequest.getTimeout() != null) {
            params.withTimeout(startRequest.getTimeout());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request stopDataFrameAnalytics(StopDataFrameAnalyticsRequest stopRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "analytics")
            .addPathPart(stopRequest.getId())
            .addPathPartAsIs("_stop")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (stopRequest.getTimeout() != null) {
            params.withTimeout(stopRequest.getTimeout());
        }
        if (stopRequest.getAllowNoMatch() != null) {
            params.putParam(
                StopDataFrameAnalyticsRequest.ALLOW_NO_MATCH.getPreferredName(), Boolean.toString(stopRequest.getAllowNoMatch()));
        }
        if (stopRequest.getForce() != null) {
            params.putParam(StopDataFrameAnalyticsRequest.FORCE.getPreferredName(), Boolean.toString(stopRequest.getForce()));
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request deleteDataFrameAnalytics(DeleteDataFrameAnalyticsRequest deleteRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "analytics")
            .addPathPart(deleteRequest.getId())
            .build();
        return new Request(HttpDelete.METHOD_NAME, endpoint);
    }

    static Request evaluateDataFrame(EvaluateDataFrameRequest evaluateRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "_evaluate")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(evaluateRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request estimateMemoryUsage(PutDataFrameAnalyticsRequest estimateRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "data_frame", "analytics", "_estimate_memory_usage")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(estimateRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request putFilter(PutFilterRequest putFilterRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("filters")
            .addPathPart(putFilterRequest.getMlFilter().getId())
            .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putFilterRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getFilter(GetFiltersRequest getFiltersRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("filters")
            .addPathPart(getFiltersRequest.getFilterId())
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        if (getFiltersRequest.getSize() != null) {
            params.putParam(PageParams.SIZE.getPreferredName(), getFiltersRequest.getSize().toString());
        }
        if (getFiltersRequest.getFrom() != null) {
            params.putParam(PageParams.FROM.getPreferredName(), getFiltersRequest.getFrom().toString());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request updateFilter(UpdateFilterRequest updateFilterRequest) throws IOException {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("filters")
            .addPathPart(updateFilterRequest.getFilterId())
            .addPathPartAsIs("_update")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(updateFilterRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteFilter(DeleteFilterRequest deleteFilterRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "filters")
            .addPathPart(deleteFilterRequest.getId())
            .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        return request;
    }

    static Request setUpgradeMode(SetUpgradeModeRequest setUpgradeModeRequest) {
        String endpoint = new EndpointBuilder().addPathPartAsIs("_ml", "set_upgrade_mode").build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.putParam(SetUpgradeModeRequest.ENABLED.getPreferredName(), Boolean.toString(setUpgradeModeRequest.isEnabled()));
        if (setUpgradeModeRequest.getTimeout() != null) {
            params.putParam(SetUpgradeModeRequest.TIMEOUT.getPreferredName(), setUpgradeModeRequest.getTimeout().toString());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request mlInfo(MlInfoRequest infoRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml", "info")
            .build();
        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    static Request findFileStructure(FindFileStructureRequest findFileStructureRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_ml")
            .addPathPartAsIs("find_file_structure")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (findFileStructureRequest.getLinesToSample() != null) {
            params.putParam(FindFileStructureRequest.LINES_TO_SAMPLE.getPreferredName(),
                findFileStructureRequest.getLinesToSample().toString());
        }
        if (findFileStructureRequest.getTimeout() != null) {
            params.putParam(FindFileStructureRequest.TIMEOUT.getPreferredName(), findFileStructureRequest.getTimeout().toString());
        }
        if (findFileStructureRequest.getCharset() != null) {
            params.putParam(FindFileStructureRequest.CHARSET.getPreferredName(), findFileStructureRequest.getCharset());
        }
        if (findFileStructureRequest.getFormat() != null) {
            params.putParam(FindFileStructureRequest.FORMAT.getPreferredName(), findFileStructureRequest.getFormat().toString());
        }
        if (findFileStructureRequest.getColumnNames() != null) {
            params.putParam(FindFileStructureRequest.COLUMN_NAMES.getPreferredName(),
                Strings.collectionToCommaDelimitedString(findFileStructureRequest.getColumnNames()));
        }
        if (findFileStructureRequest.getHasHeaderRow() != null) {
            params.putParam(FindFileStructureRequest.HAS_HEADER_ROW.getPreferredName(),
                findFileStructureRequest.getHasHeaderRow().toString());
        }
        if (findFileStructureRequest.getDelimiter() != null) {
            params.putParam(FindFileStructureRequest.DELIMITER.getPreferredName(),
                findFileStructureRequest.getDelimiter().toString());
        }
        if (findFileStructureRequest.getQuote() != null) {
            params.putParam(FindFileStructureRequest.QUOTE.getPreferredName(), findFileStructureRequest.getQuote().toString());
        }
        if (findFileStructureRequest.getShouldTrimFields() != null) {
            params.putParam(FindFileStructureRequest.SHOULD_TRIM_FIELDS.getPreferredName(),
                findFileStructureRequest.getShouldTrimFields().toString());
        }
        if (findFileStructureRequest.getGrokPattern() != null) {
            params.putParam(FindFileStructureRequest.GROK_PATTERN.getPreferredName(), findFileStructureRequest.getGrokPattern());
        }
        if (findFileStructureRequest.getTimestampFormat() != null) {
            params.putParam(FindFileStructureRequest.TIMESTAMP_FORMAT.getPreferredName(), findFileStructureRequest.getTimestampFormat());
        }
        if (findFileStructureRequest.getTimestampField() != null) {
            params.putParam(FindFileStructureRequest.TIMESTAMP_FIELD.getPreferredName(), findFileStructureRequest.getTimestampField());
        }
        if (findFileStructureRequest.getExplain() != null) {
            params.putParam(FindFileStructureRequest.EXPLAIN.getPreferredName(), findFileStructureRequest.getExplain().toString());
        }
        request.addParameters(params.asMap());
        BytesReference sample = findFileStructureRequest.getSample();
        BytesRef source = sample.toBytesRef();
        HttpEntity byteEntity = new NByteArrayEntity(source.bytes, source.offset, source.length, createContentType(XContentType.JSON));
        request.setEntity(byteEntity);
        return request;
    }
}
