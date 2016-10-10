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
package org.elasticsearch.script.mustache;

import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;
import org.elasticsearch.script.Script.StoredScriptSource;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestPutSearchTemplateAction extends BaseRestHandler {

    @Inject
    public RestPutSearchTemplateAction(Settings settings, RestController controller) {
        super(settings);

        controller.registerHandler(POST, "/_search/template/{id}", this);
        controller.registerHandler(PUT, "/_search/template/{id}", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        StoredScriptSource source;

        try (XContentParser parser = XContentHelper.createParser(request.content())) {
            source = StoredScriptSource.parse(parser);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }

        PutStoredScriptRequest putRequest = new PutStoredScriptRequest(request.param("id"), source);
        return channel -> client.admin().cluster().putStoredScript(putRequest, new AcknowledgedRestListener<>(channel));
    }


}
