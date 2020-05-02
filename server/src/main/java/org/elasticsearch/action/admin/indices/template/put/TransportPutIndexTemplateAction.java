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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Put index template action.
 */
public class TransportPutIndexTemplateAction extends TransportMasterNodeAction<PutIndexTemplateRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutIndexTemplateAction.class);

    private final MetadataIndexTemplateService indexTemplateService;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public TransportPutIndexTemplateAction(TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, MetadataIndexTemplateService indexTemplateService,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                           IndexScopedSettings indexScopedSettings) {
        super(PutIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            PutIndexTemplateRequest::new, indexNameExpressionResolver);
        this.indexTemplateService = indexTemplateService;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PutIndexTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(Task task, final PutIndexTemplateRequest request, final ClusterState state,
                                   final ActionListener<AcknowledgedResponse> listener) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }
        final Settings.Builder templateSettingsBuilder = Settings.builder();
        templateSettingsBuilder.put(request.settings()).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
        indexScopedSettings.validate(templateSettingsBuilder.build(), true); // templates must be consistent with regards to dependencies
        indexTemplateService.putTemplate(new MetadataIndexTemplateService.PutRequest(cause, request.name())
                .patterns(request.patterns())
                .order(request.order())
                .settings(templateSettingsBuilder.build())
                .mappings(request.mappings())
                .aliases(request.aliases())
                .create(request.create())
                .masterTimeout(request.masterNodeTimeout())
                .version(request.version()),

                new MetadataIndexTemplateService.PutListener() {
                    @Override
                    public void onResponse(MetadataIndexTemplateService.PutResponse response) {
                        listener.onResponse(new AcknowledgedResponse(response.acknowledged()));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(() -> new ParameterizedMessage("failed to put template [{}]", request.name()), e);
                        listener.onFailure(e);
                    }
                });
    }
}
