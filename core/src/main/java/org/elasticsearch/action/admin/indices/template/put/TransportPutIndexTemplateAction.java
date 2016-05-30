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
package org.elasticsearch.action.admin.indices.template.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Put index template action.
 */
public class TransportPutIndexTemplateAction extends TransportMasterNodeAction<PutIndexTemplateRequest, PutIndexTemplateResponse> {

    private final MetaDataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public TransportPutIndexTemplateAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, MetaDataIndexTemplateService indexTemplateService,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, IndexScopedSettings indexScopedSettings) {
        super(settings, PutIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, PutIndexTemplateRequest::new);
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutIndexTemplateResponse newResponse() {
        return new PutIndexTemplateResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutIndexTemplateRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, "");
    }

    @Override
    protected void masterOperation(final PutIndexTemplateRequest request, final ClusterState state, final ActionListener<PutIndexTemplateResponse> listener) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }
        final Settings.Builder templateSettingsBuilder = Settings.builder();
        templateSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetaData.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(templateSettingsBuilder);
        indexTemplateService.putTemplate(new MetaDataIndexTemplateService.PutRequest(cause, request.name())
                .template(request.template())
                .order(request.order())
                .settings(templateSettingsBuilder.build())
                .mappings(request.mappings())
                .aliases(request.aliases())
                .customs(request.customs())
                .create(request.create())
                .masterTimeout(request.masterNodeTimeout()),

                new MetaDataIndexTemplateService.PutListener() {
                    @Override
                    public void onResponse(MetaDataIndexTemplateService.PutResponse response) {
                        listener.onResponse(new PutIndexTemplateResponse(response.acknowledged()));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.debug("failed to put template [{}]", t, request.name());
                        listener.onFailure(t);
                    }
                });
    }
}
