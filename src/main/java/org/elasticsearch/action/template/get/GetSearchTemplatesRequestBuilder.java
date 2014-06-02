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
package org.elasticsearch.action.template.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.internal.InternalGenericClient;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.script.ScriptService;

/**
 *
 */
public class GetSearchTemplatesRequestBuilder extends MasterNodeReadOperationRequestBuilder<GetSearchTemplatesRequest, GetSearchTemplatesResponse, GetSearchTemplatesRequestBuilder> {

    Client searchClient;
    ESLogger logger = Loggers.getLogger(GetSearchTemplatesRequestBuilder.class);
    public GetSearchTemplatesRequestBuilder(Client client) {
        super((InternalGenericClient) client, new GetSearchTemplatesRequest());
        this.searchClient = client;
    }

    public GetSearchTemplatesRequestBuilder(Client client, String... ids) {
        super((InternalGenericClient) client, new GetSearchTemplatesRequest(ids));
        this.searchClient = client;
    }

    @Override
    protected void doExecute(ActionListener<GetSearchTemplatesResponse> listener) {
        GetSearchTemplatesResponse response = null;
        try {
            String script = ScriptService.getScriptFromIndex(searchClient,  ScriptService.SCRIPT_INDEX, "mustache", request.ids()[0]);
            response = new GetSearchTemplatesResponse(script);
        } catch( ElasticsearchException ee ){
            logger.error("Failed to find " + ee.toString());
        }
        listener.onResponse(response);
    }
}
