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

package org.elasticsearch.action.admin.indices.migrate;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.script.Script;

/**
 * Request to migrate data from one index into a new index.
 */
public class MigrateIndexRequestBuilder
        extends AcknowledgedRequestBuilder<MigrateIndexRequest, MigrateIndexResponse, MigrateIndexRequestBuilder> {

    public MigrateIndexRequestBuilder(ElasticsearchClient client,
            Action<MigrateIndexRequest, MigrateIndexResponse, MigrateIndexRequestBuilder> action) {
        super(client, action, new MigrateIndexRequest());
    }

    /**
     * Set the index to migrate from.
     */
    public MigrateIndexRequestBuilder setSourceIndex(String sourceIndex) {
        request.setSourceIndex(sourceIndex);
        return this;
    }

    /**
     * Set the script used to migrate documents.
     */
    public MigrateIndexRequestBuilder setScript(Script script) {
        request.setScript(script);
        return this;
    }

    /**
     * Set the request used to create the new index.
     */
    public MigrateIndexRequestBuilder setCreateIndexRequest(CreateIndexRequest createIndexRequest) {
        request.setCreateIndexRequest(createIndexRequest);
        return this;
    }

    /**
     * Set the aliases to create.
     */
    public MigrateIndexRequestBuilder setAliases(String... aliases) {
        for (String alias: aliases) {
            request.getCreateIndexRequest().alias(new Alias(alias));
        }
        return this;
    }

    /**
     * Set the aliases to create.
     */
    public MigrateIndexRequestBuilder setAliases(Alias... aliases) {
        for (Alias alias: aliases) {
            request.getCreateIndexRequest().alias(alias);
        }
        return this;
    }
}
