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

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.resolveV2Mappings;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findV2Template;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.resolveSettings;

public class TransportSimulateIndexTemplateAction
    extends TransportMasterNodeReadAction<SimulateIndexTemplateRequest, SimulateIndexTemplateResponse> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportSimulateIndexTemplateAction(TransportService transportService, ClusterService clusterService,
                                                ThreadPool threadPool, MetadataIndexTemplateService indexTemplateService,
                                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                                NamedXContentRegistry xContentRegistry) {
        super(PostSimulateIndexTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            SimulateIndexTemplateRequest::new, indexNameExpressionResolver);
        this.indexTemplateService = indexTemplateService;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected SimulateIndexTemplateResponse read(StreamInput in) throws IOException {
        return new SimulateIndexTemplateResponse(in);
    }

    @Override
    protected void masterOperation(Task task, SimulateIndexTemplateRequest request, ClusterState state,
                                   ActionListener<SimulateIndexTemplateResponse> listener) throws Exception {
        ClusterState simulateOnClusterState = state;
        if (request.getIndexTemplateRequest() != null) {
            // we'll "locally" add the template defined by the user in the cluster state (as if it existed in the system)
            String simulateTemplateToAdd = "simulate_new_template_" + UUIDs.randomBase64UUID();
            simulateOnClusterState = indexTemplateService.addIndexTemplateV2(state, request.getIndexTemplateRequest().create(),
                simulateTemplateToAdd, request.getIndexTemplateRequest().indexTemplate());
        }

        String matchingTemplate = findV2Template(simulateOnClusterState.metadata(), request.getIndexName(), false);
        Settings settings = resolveSettings(simulateOnClusterState.metadata(), matchingTemplate);

        // empty request mapping as the user can't specify any explicit mappings via the simulate api
        Map<String, Object> mappings = resolveV2Mappings("{}", simulateOnClusterState, matchingTemplate, xContentRegistry);
        String mappingsJson = Strings.toString(XContentFactory.jsonBuilder()
            .startObject()
            .field(MapperService.SINGLE_MAPPING_NAME, mappings)
            .endObject());

        List<Map<String, AliasMetadata>> resolvedAliases = MetadataIndexTemplateService.resolveAliases(simulateOnClusterState.metadata(),
            matchingTemplate);
        Map<String, AliasMetadata> aliases = new HashMap<>();
        for (Map<String, AliasMetadata> alias : resolvedAliases) {
            for (Map.Entry<String, AliasMetadata> aliasEntry : alias.entrySet()) {
                if (aliases.containsKey(aliasEntry.getKey()) == false) {
                    aliases.put(aliasEntry.getKey(), aliasEntry.getValue());
                }
            }
        }

        IndexTemplateV2 templateV2 = simulateOnClusterState.metadata().templatesV2().get(matchingTemplate);
        assert templateV2 != null : "the matched template must exist";

        Map<String, List<String>> conflictingV1Templates = MetadataIndexTemplateService.findConflictingV1Templates(simulateOnClusterState
            , matchingTemplate, templateV2.indexPatterns());
        Template template = new Template(settings, mappingsJson == null ? null : new CompressedXContent(mappingsJson), aliases);
        listener.onResponse(new SimulateIndexTemplateResponse(template, conflictingV1Templates));
    }

    @Override
    protected ClusterBlockException checkBlock(SimulateIndexTemplateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
