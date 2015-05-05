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

package org.elasticsearch.action.indexedscripts.delete;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.index.VersionType;

/**
 * A delete document action request builder.
 */
public class DeleteIndexedScriptRequestBuilder extends ActionRequestBuilder<DeleteIndexedScriptRequest, DeleteIndexedScriptResponse, DeleteIndexedScriptRequestBuilder> {

    public DeleteIndexedScriptRequestBuilder(ElasticsearchClient client, DeleteIndexedScriptAction action) {
        super(client, action, new DeleteIndexedScriptRequest());
    }

    /**
     * Sets the language of the script to delete.
     */
    public DeleteIndexedScriptRequestBuilder setScriptLang(String scriptLang) {
        request.scriptLang(scriptLang);
        return this;
    }

    /**
     * Sets the id of the document to delete.
     */
    public DeleteIndexedScriptRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Sets the type of versioning to use. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public DeleteIndexedScriptRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }
}
