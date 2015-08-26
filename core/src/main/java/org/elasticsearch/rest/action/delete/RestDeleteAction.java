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

package org.elasticsearch.rest.action.delete;

import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;

/**
 *
 */
public class RestDeleteAction extends BaseRestHandler {

    @Inject
    public RestDeleteAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(DELETE, "/{index}/{type}/{id}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        DeleteRequest deleteRequest = new DeleteRequest(request.param("index"), request.param("type"), request.param("id"));
        deleteRequest.routing(request.param("routing"));
        deleteRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
        deleteRequest.timeout(request.paramAsTime("timeout", DeleteRequest.DEFAULT_TIMEOUT));
        deleteRequest.refresh(request.paramAsBoolean("refresh", deleteRequest.refresh()));
        deleteRequest.version(RestActions.parseVersion(request));
        deleteRequest.versionType(VersionType.fromString(request.param("version_type"), deleteRequest.versionType()));

        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            deleteRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }

        client.delete(deleteRequest, new RestBuilderListener<DeleteResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteResponse result, XContentBuilder builder) throws Exception {
                ActionWriteResponse.ShardInfo shardInfo = result.getShardInfo();
                builder.startObject().field(Fields.FOUND, result.isFound())
                        .field(Fields._INDEX, result.getIndex())
                        .field(Fields._TYPE, result.getType())
                        .field(Fields._ID, result.getId())
                        .field(Fields._VERSION, result.getVersion())
                        .value(shardInfo)
                        .endObject();
                RestStatus status = shardInfo.status();
                if (!result.isFound()) {
                    status = NOT_FOUND;
                }
                return new BytesRestResponse(status, builder);
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString FOUND = new XContentBuilderString("found");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
    }
}
