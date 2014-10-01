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

package org.elasticsearch.action.indexedscripts.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.VersionType;

/**
 * A get document action request builder.
 */
public class GetIndexedScriptRequestBuilder extends ActionRequestBuilder<GetIndexedScriptRequest, GetIndexedScriptResponse, GetIndexedScriptRequestBuilder, Client> {


    public GetIndexedScriptRequestBuilder(Client client) {
        super(client, new GetIndexedScriptRequest());
    }

    /**
     * Sets the type of the document to fetch. If set to <tt>null</tt>, will use just the id to fetch the
     * first document matching it.
     */
    public GetIndexedScriptRequestBuilder setScriptLang(@Nullable String type) {
        request.scriptLang(type);
        return this;
    }

    /**
     * Sets the id of the document to fetch.
     */
    public GetIndexedScriptRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Sets the version, which will cause the get operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public GetIndexedScriptRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public GetIndexedScriptRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<GetIndexedScriptResponse> listener) {
        client.getIndexedScript(request, listener);
    }

}
