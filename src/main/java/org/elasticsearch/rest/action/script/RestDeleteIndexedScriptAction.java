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
package org.elasticsearch.rest.action.script;

import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptRequest;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;
import static org.elasticsearch.rest.RestStatus.NOT_FOUND;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestDeleteIndexedScriptAction extends BaseRestHandler {

    @Inject
    public RestDeleteIndexedScriptAction(Settings settings, RestController controller, Client client) {
        this(settings, controller, true, client);
    }

    protected RestDeleteIndexedScriptAction(Settings settings, RestController controller, boolean registerDefaultHandlers, Client client) {
        super(settings, controller, client);
        if (registerDefaultHandlers) {
            controller.registerHandler(DELETE, "/_scripts/{lang}/{id}", this);
        }
    }

    protected String getScriptLang(RestRequest request) {
        return request.param("lang");
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, Client client) {
        DeleteIndexedScriptRequest deleteIndexedScriptRequest = new DeleteIndexedScriptRequest(getScriptLang(request), request.param("id"));
        deleteIndexedScriptRequest.version(request.paramAsLong("version", deleteIndexedScriptRequest.version()));
        deleteIndexedScriptRequest.versionType(VersionType.fromString(request.param("version_type"), deleteIndexedScriptRequest.versionType()));
        client.deleteIndexedScript(deleteIndexedScriptRequest, new RestBuilderListener<DeleteIndexedScriptResponse>(channel) {
            @Override
            public RestResponse buildResponse(DeleteIndexedScriptResponse result, XContentBuilder builder) throws Exception {
                builder.startObject()
                        .field(Fields.FOUND, result.isFound())
                        .field(Fields._INDEX, result.getIndex())
                        .field(Fields._TYPE, result.getType())
                        .field(Fields._ID, result.getId())
                        .field(Fields._VERSION, result.getVersion())
                        .endObject();
                RestStatus status = OK;
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