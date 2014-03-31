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

package org.elasticsearch.rest.action.admin.indices.settings;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetSettingsAction extends BaseRestHandler {

    @Inject
    public RestGetSettingsAction(Settings settings, Client client, RestController controller) {
        super(settings, client);
        controller.registerHandler(GET, "/_settings", this);
        controller.registerHandler(GET, "/{index}/_settings", this);
        controller.registerHandler(GET, "/{index}/_settings/{name}", this);
        controller.registerHandler(GET, "/_settings/{name}", this);
        controller.registerHandler(GET, "/{index}/_setting/{name}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        final String[] names = request.paramAsStringArrayOrEmptyIfAll("name");
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
                .indices(Strings.splitStringByCommaToArray(request.param("index")))
                .indicesOptions(IndicesOptions.fromRequest(request, IndicesOptions.strict()))
                .names(names);
        getSettingsRequest.local(request.paramAsBoolean("local", getSettingsRequest.local()));

        client.admin().indices().getSettings(getSettingsRequest, new ActionListener<GetSettingsResponse>() {

            @Override
            public void onResponse(GetSettingsResponse getSettingsResponse) {
                try {
                    XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
                    builder.startObject();
                    for (ObjectObjectCursor<String, Settings> cursor : getSettingsResponse.getIndexToSettings()) {
                        // no settings, jump over it to shorten the response data
                        if (cursor.value.getAsMap().isEmpty()) {
                            continue;
                        }
                        builder.startObject(cursor.key, XContentBuilder.FieldCaseConversion.NONE);
                        builder.startObject(Fields.SETTINGS);
                        cursor.value.toXContent(builder, request);
                        builder.endObject();
                        builder.endObject();
                    }
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(OK, builder));
                } catch (IOException e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new BytesRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    static class Fields {

        static final XContentBuilderString SETTINGS = new XContentBuilderString("settings");

    }
}
