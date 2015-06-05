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

package org.elasticsearch.rest.action.update;

import org.elasticsearch.action.ActionWriteResponse;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.CREATED;

/**
 */
public class RestUpdateAction extends BaseRestHandler {

    @Inject
    public RestUpdateAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(POST, "/{index}/{type}/{id}/_update", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest(request.param("index"), request.param("type"), request.param("id"));
        updateRequest.routing(request.param("routing"));
        updateRequest.parent(request.param("parent"));
        updateRequest.timeout(request.paramAsTime("timeout", updateRequest.timeout()));
        updateRequest.refresh(request.paramAsBoolean("refresh", updateRequest.refresh()));
        String consistencyLevel = request.param("consistency");
        if (consistencyLevel != null) {
            updateRequest.consistencyLevel(WriteConsistencyLevel.fromString(consistencyLevel));
        }
        updateRequest.docAsUpsert(request.paramAsBoolean("doc_as_upsert", updateRequest.docAsUpsert()));
        ScriptParameterParser scriptParameterParser = new ScriptParameterParser();
        scriptParameterParser.parseParams(request);
        ScriptParameterValue scriptValue = scriptParameterParser.getDefaultScriptParameterValue();
        if (scriptValue != null) {
            Map<String, Object> scriptParams = new HashMap<>();
            for (Map.Entry<String, String> entry : request.params().entrySet()) {
                if (entry.getKey().startsWith("sp_")) {
                    scriptParams.put(entry.getKey().substring(3), entry.getValue());
                }
            }
            updateRequest.script(new Script(scriptValue.script(), scriptValue.scriptType(), scriptParameterParser.lang(), scriptParams));
        }
        String sField = request.param("fields");
        if (sField != null) {
            String[] sFields = Strings.splitStringByCommaToArray(sField);
            if (sFields != null) {
                updateRequest.fields(sFields);
            }
        }
        updateRequest.retryOnConflict(request.paramAsInt("retry_on_conflict", updateRequest.retryOnConflict()));
        updateRequest.version(RestActions.parseVersion(request));
        updateRequest.versionType(VersionType.fromString(request.param("version_type"), updateRequest.versionType()));


        // see if we have it in the body
        if (request.hasContent()) {
            updateRequest.source(request.content());
            IndexRequest upsertRequest = updateRequest.upsertRequest();
            if (upsertRequest != null) {
                upsertRequest.routing(request.param("routing"));
                upsertRequest.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
                upsertRequest.timestamp(request.param("timestamp"));
                if (request.hasParam("ttl")) {
                    upsertRequest.ttl(request.paramAsTime("ttl", null).millis());
                }
                upsertRequest.version(RestActions.parseVersion(request));
                upsertRequest.versionType(VersionType.fromString(request.param("version_type"), upsertRequest.versionType()));
            }
            IndexRequest doc = updateRequest.doc();
            if (doc != null) {
                doc.routing(request.param("routing"));
                doc.parent(request.param("parent")); // order is important, set it after routing, so it will set the routing
                doc.timestamp(request.param("timestamp"));
                if (request.hasParam("ttl")) {
                    doc.ttl(request.paramAsTime("ttl", null).millis());
                }
                doc.version(RestActions.parseVersion(request));
                doc.versionType(VersionType.fromString(request.param("version_type"), doc.versionType()));
            }
        }

        client.update(updateRequest, new RestBuilderListener<UpdateResponse>(channel) {
            @Override
            public RestResponse buildResponse(UpdateResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                ActionWriteResponse.ShardInfo shardInfo = response.getShardInfo();
                builder.field(Fields._INDEX, response.getIndex())
                        .field(Fields._TYPE, response.getType())
                        .field(Fields._ID, response.getId())
                        .field(Fields._VERSION, response.getVersion());

                shardInfo.toXContent(builder, request);
                if (response.getGetResult() != null) {
                    builder.startObject(Fields.GET);
                    response.getGetResult().toXContentEmbedded(builder, request);
                    builder.endObject();
                }

                builder.endObject();
                RestStatus status = shardInfo.status();
                if (response.isCreated()) {
                    status = CREATED;
                }
                return new BytesRestResponse(status, builder);
            }
        });
    }

    static final class Fields {
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _VERSION = new XContentBuilderString("_version");
        static final XContentBuilderString GET = new XContentBuilderString("get");
    }
}
