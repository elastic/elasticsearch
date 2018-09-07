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

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.RequestConverters.EndpointBuilder;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.common.Strings;

import java.io.IOException;

import static org.elasticsearch.client.RequestConverters.REQUEST_BODY_CONTENT_TYPE;
import static org.elasticsearch.client.RequestConverters.createEntity;

final class MLRequestConverters {

    private MLRequestConverters() {}

    static Request putJob(PutJobRequest putJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(putJobRequest.getJob().getId())
                .build();
        Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        request.setEntity(createEntity(putJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getJob(GetJobRequest getJobRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_xpack")
            .addPathPartAsIs("ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(Strings.collectionToCommaDelimitedString(getJobRequest.getJobIds()))
            .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params(request);
        if (getJobRequest.isAllowNoJobs() != null) {
            params.putParam("allow_no_jobs", Boolean.toString(getJobRequest.isAllowNoJobs()));
        }

        return request;
    }

    static Request getJobStats(GetJobStatsRequest getJobStatsRequest) {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(Strings.collectionToCommaDelimitedString(getJobStatsRequest.getJobIds()))
                .addPathPartAsIs("_stats")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params(request);
        if (getJobStatsRequest.isAllowNoJobs() != null) {
            params.putParam("allow_no_jobs", Boolean.toString(getJobStatsRequest.isAllowNoJobs()));
        }
        return request;
    }

    static Request openJob(OpenJobRequest openJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
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
            .addPathPartAsIs("_xpack")
            .addPathPartAsIs("ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(Strings.collectionToCommaDelimitedString(closeJobRequest.getJobIds()))
            .addPathPartAsIs("_close")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(closeJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request deleteJob(DeleteJobRequest deleteJobRequest) {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(deleteJobRequest.getJobId())
                .build();
        Request request = new Request(HttpDelete.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params(request);
        params.putParam("force", Boolean.toString(deleteJobRequest.isForce()));

        return request;
    }

    static Request flushJob(FlushJobRequest flushJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(flushJobRequest.getJobId())
                .addPathPartAsIs("_flush")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(flushJobRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request updateJob(UpdateJobRequest updateJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(updateJobRequest.getJobUpdate().getJobId())
                .addPathPartAsIs("_update")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setEntity(createEntity(updateJobRequest.getJobUpdate(), REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getBuckets(GetBucketsRequest getBucketsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(getBucketsRequest.getJobId())
                .addPathPartAsIs("results")
                .addPathPartAsIs("buckets")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getBucketsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getOverallBuckets(GetOverallBucketsRequest getOverallBucketsRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
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
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(getRecordsRequest.getJobId())
                .addPathPartAsIs("results")
                .addPathPartAsIs("records")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getRecordsRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }

    static Request getInfluencers(GetInfluencersRequest getInfluencersRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(getInfluencersRequest.getJobId())
                .addPathPartAsIs("results")
                .addPathPartAsIs("influencers")
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        request.setEntity(createEntity(getInfluencersRequest, REQUEST_BODY_CONTENT_TYPE));
        return request;
    }
}
