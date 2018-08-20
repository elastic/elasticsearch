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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.RequestConverters.EndpointBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.protocol.xpack.ml.CloseJobRequest;
import org.elasticsearch.protocol.xpack.ml.DeleteJobRequest;
import org.elasticsearch.protocol.xpack.ml.OpenJobRequest;
import org.elasticsearch.protocol.xpack.ml.PutJobRequest;

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

    static Request openJob(OpenJobRequest openJobRequest) throws IOException {
        String endpoint = new EndpointBuilder()
                .addPathPartAsIs("_xpack")
                .addPathPartAsIs("ml")
                .addPathPartAsIs("anomaly_detectors")
                .addPathPart(openJobRequest.getJobId())
                .addPathPartAsIs("_open")
                .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setJsonEntity(openJobRequest.toString());
        return request;
    }

    static Request closeJob(CloseJobRequest closeJobRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_xpack")
            .addPathPartAsIs("ml")
            .addPathPartAsIs("anomaly_detectors")
            .addPathPart(Strings.collectionToCommaDelimitedString(closeJobRequest.getJobIds()))
            .addPathPartAsIs("_close")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params(request);
        if (closeJobRequest.isForce() != null) {
            params.putParam("force", Boolean.toString(closeJobRequest.isForce()));
        }
        if (closeJobRequest.isAllowNoJobs() != null) {
            params.putParam("allow_no_jobs", Boolean.toString(closeJobRequest.isAllowNoJobs()));
        }
        if (closeJobRequest.getTimeout() != null) {
            params.putParam("timeout", closeJobRequest.getTimeout().getStringRep());
        }

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
}
