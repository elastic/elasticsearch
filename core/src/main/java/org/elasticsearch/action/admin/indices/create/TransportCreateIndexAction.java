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

package org.elasticsearch.action.admin.indices.create;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create index action.
 */
public class TransportCreateIndexAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {

    private final MetaDataCreateIndexService createIndexService;

    @Inject
    public TransportCreateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, MetaDataCreateIndexService createIndexService,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, CreateIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, CreateIndexRequest::new);
        this.createIndexService = createIndexService;
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateIndexResponse newResponse() {
        return new CreateIndexResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    protected void masterOperation(final CreateIndexRequest request, final ClusterState state, final ActionListener<CreateIndexResponse> listener) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        String providedIndexName = inferIndexNameFromAlias(request.index(), state);
        final String indexName = indexNameExpressionResolver.resolveDateMathExpression(providedIndexName);

        final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(request, cause, indexName, providedIndexName, request.updateAllTypes())
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                .settings(request.settings()).mappings(request.mappings())
                .aliases(request.aliases()).customs(request.customs())
                .waitForActiveShards(request.waitForActiveShards());

        createIndexService.createIndex(updateRequest, ActionListener.wrap(response ->
            listener.onResponse(new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcked())),
            listener::onFailure));
    }

    private String inferIndexNameFromAlias(String providedIndexName, ClusterState state) {
        final String MATCH_ALL_GROUP = "(.*)";
        final String GROUP_BACK_REFERENCE = "\\\\1";
        final String indexNamePlaceHolder = "{index}";
        final String indexNamePlaceholderForRegex = "\\" + indexNamePlaceHolder;

        final String indexName = indexNameExpressionResolver.resolveDateMathExpression(providedIndexName);

        for (IndexTemplateMetaData templateMetadata : getAllTemplatesByOrder(state)) {
            if (templateMetadata.inferIndexNameFromAlias()) {
                for (ObjectCursor<String> stringObjectCursor : templateMetadata.aliases().keys()) {
                    String aliasName = stringObjectCursor.value;
                    if (aliasName.contains(indexNamePlaceHolder)) {
                        for (String template : templateMetadata.patterns()) {
                            if (Regex.simpleMatch(aliasName.replace(indexNamePlaceHolder, template), indexName)) {
                                String aliasNameForIndexRegex = aliasName.replaceFirst(indexNamePlaceholderForRegex, MATCH_ALL_GROUP).replaceAll(indexNamePlaceholderForRegex, GROUP_BACK_REFERENCE);
                                Pattern aliasPattern = Regex.compile(aliasNameForIndexRegex);

                                if (providedIndexName.startsWith("<") && providedIndexName.endsWith(">")) {
                                    Matcher providedIndexNameMatcher = aliasPattern.matcher(providedIndexName.substring(1, providedIndexName.length() - 1));
                                    if (providedIndexNameMatcher.matches()) {
                                        return "<" + providedIndexNameMatcher.group(1) + ">";
                                    }
                                } else {
                                    Matcher providedIndexNameMatcher = aliasPattern.matcher(providedIndexName);
                                    if (providedIndexNameMatcher.matches()) {
                                        return providedIndexNameMatcher.group(1);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return providedIndexName;
    }

    private List<IndexTemplateMetaData> getAllTemplatesByOrder(ClusterState state) {
        List<IndexTemplateMetaData> templateMetadata = new ArrayList<>();
        for (ObjectCursor<IndexTemplateMetaData> cursor : state.metaData().templates().values()) {
            IndexTemplateMetaData metadata = cursor.value;
            templateMetadata.add(metadata);
        }

        CollectionUtil.timSort(templateMetadata, Comparator.comparingInt(IndexTemplateMetaData::order).reversed());
        return templateMetadata;
    }
}
