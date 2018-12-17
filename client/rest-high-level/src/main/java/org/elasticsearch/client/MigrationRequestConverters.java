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

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.client.migration.IndexUpgradeInfoRequest;
import org.elasticsearch.client.migration.IndexUpgradeRequest;

final class MigrationRequestConverters {

    private MigrationRequestConverters() {
    }

    static Request getMigrationAssistance(IndexUpgradeInfoRequest indexUpgradeInfoRequest) {
        RequestConverters.EndpointBuilder endpointBuilder = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_migration", "assistance")
            .addCommaSeparatedPathParts(indexUpgradeInfoRequest.indices());
        String endpoint = endpointBuilder.build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params(request);
        parameters.withIndicesOptions(indexUpgradeInfoRequest.indicesOptions());
        return request;
    }

    static Request migrate(IndexUpgradeRequest indexUpgradeRequest) {
        return prepareMigrateRequest(indexUpgradeRequest, true);
    }

    static Request submitMigrateTask(IndexUpgradeRequest indexUpgradeRequest) {
        return prepareMigrateRequest(indexUpgradeRequest, false);
    }

    static Request getDeprecationInfo(DeprecationInfoRequest deprecationInfoRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addCommaSeparatedPathParts(deprecationInfoRequest.getIndices())
            .addPathPartAsIs("_migration", "deprecations")
            .build();

        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    private static Request prepareMigrateRequest(IndexUpgradeRequest indexUpgradeRequest, boolean waitForCompletion) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addPathPartAsIs("_migration", "upgrade")
            .addPathPart(indexUpgradeRequest.index())
            .build();

        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params(request)
            .withWaitForCompletion(waitForCompletion);

        return request;
    }
}
