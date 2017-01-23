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
package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.AcknowledgedRestListener;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteStoredScriptAction extends BaseRestHandler {

    public RestDeleteStoredScriptAction(Settings settings, RestController controller) {
        super(settings);

        // Note {lang} is actually {id} in the first handler.  It appears
        // parameters as part of the path must be of the same ordering relative
        // to name or they will not work as expected.
        controller.registerHandler(DELETE, "/_scripts/{lang}", this);
        controller.registerHandler(DELETE, "/_scripts/{lang}/{id}", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String id = request.param("id");
        String lang = request.param("lang");

        // In the case where only {lang} is not null, we make it {id} because of
        // name ordering issues in the handlers' paths.
        if (id == null) {
            id = lang;
            lang = null;
        }

        if (lang != null) {
            deprecationLogger.deprecated(
                "specifying lang [" + lang + "] as part of the url path is deprecated");
        }

        DeleteStoredScriptRequest deleteStoredScriptRequest = new DeleteStoredScriptRequest(id, lang);
        return channel -> client.admin().cluster().deleteStoredScript(deleteStoredScriptRequest, new AcknowledgedRestListener<>(channel));
    }
}
