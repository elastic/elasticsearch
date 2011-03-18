/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.client.support;

import org.elasticsearch.client.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.client.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.elasticsearch.client.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.client.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.client.action.admin.indices.flush.FlushRequestBuilder;
import org.elasticsearch.client.action.admin.indices.gateway.snapshot.GatewaySnapshotRequestBuilder;
import org.elasticsearch.client.action.admin.indices.mapping.delete.DeleteMappingRequestBuilder;
import org.elasticsearch.client.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.client.action.admin.indices.open.OpenIndexRequestBuilder;
import org.elasticsearch.client.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.client.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.client.action.admin.indices.settings.UpdateSettingsRequestBuilder;
import org.elasticsearch.client.action.admin.indices.status.IndicesStatusRequestBuilder;
import org.elasticsearch.client.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.elasticsearch.client.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.client.internal.InternalIndicesAdminClient;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractIndicesAdminClient implements InternalIndicesAdminClient {

    @Override public IndicesAliasesRequestBuilder prepareAliases() {
        return new IndicesAliasesRequestBuilder(this);
    }

    @Override public ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
        return new ClearIndicesCacheRequestBuilder(this).setIndices(indices);
    }

    @Override public CreateIndexRequestBuilder prepareCreate(String index) {
        return new CreateIndexRequestBuilder(this, index);
    }

    @Override public DeleteIndexRequestBuilder prepareDelete(String... indices) {
        return new DeleteIndexRequestBuilder(this, indices);
    }

    @Override public CloseIndexRequestBuilder prepareClose(String index) {
        return new CloseIndexRequestBuilder(this, index);
    }

    @Override public OpenIndexRequestBuilder prepareOpen(String index) {
        return new OpenIndexRequestBuilder(this, index);
    }

    @Override public FlushRequestBuilder prepareFlush(String... indices) {
        return new FlushRequestBuilder(this).setIndices(indices);
    }

    @Override public GatewaySnapshotRequestBuilder prepareGatewaySnapshot(String... indices) {
        return new GatewaySnapshotRequestBuilder(this).setIndices(indices);
    }

    @Override public PutMappingRequestBuilder preparePutMapping(String... indices) {
        return new PutMappingRequestBuilder(this).setIndices(indices);
    }

    @Override public DeleteMappingRequestBuilder prepareDeleteMapping(String... indices) {
        return new DeleteMappingRequestBuilder(this).setIndices(indices);
    }

    @Override public OptimizeRequestBuilder prepareOptimize(String... indices) {
        return new OptimizeRequestBuilder(this).setIndices(indices);
    }

    @Override public RefreshRequestBuilder prepareRefresh(String... indices) {
        return new RefreshRequestBuilder(this).setIndices(indices);
    }

    @Override public IndicesStatusRequestBuilder prepareStatus(String... indices) {
        return new IndicesStatusRequestBuilder(this).setIndices(indices);
    }

    @Override public UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
        return new UpdateSettingsRequestBuilder(this).setIndices(indices);
    }

    @Override public AnalyzeRequestBuilder prepareAnalyze(String index, String text) {
        return new AnalyzeRequestBuilder(this, index, text);
    }

    @Override public PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
        return new PutIndexTemplateRequestBuilder(this, name);
    }

    @Override public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
        return new DeleteIndexTemplateRequestBuilder(this, name);
    }
}
