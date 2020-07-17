/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findConflictingV1Templates;
import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.findConflictingV2Templates;

/**
 * Handles simulating an index template either by name (looking it up in the
 * cluster state), or by a provided template configuration
 */
public class TransportSimulateTemplateAction
    extends TransportMasterNodeReadAction<SimulateTemplateAction.Request, SimulateIndexTemplateResponse> {

    private final MetadataIndexTemplateService indexTemplateService;
    private final NamedXContentRegistry xContentRegistry;
    private final IndicesService indicesService;
    private AliasValidator aliasValidator;

    @Inject
    public TransportSimulateTemplateAction(TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, MetadataIndexTemplateService indexTemplateService,
                                           ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                           NamedXContentRegistry xContentRegistry, IndicesService indicesService) {
        super(SimulateTemplateAction.NAME, transportService, clusterService, threadPool, actionFilters,
            SimulateTemplateAction.Request::new, indexNameExpressionResolver);
        this.indexTemplateService = indexTemplateService;
        this.xContentRegistry = xContentRegistry;
        this.indicesService = indicesService;
        this.aliasValidator = new AliasValidator();
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
    protected void masterOperation(Task task, SimulateTemplateAction.Request request, ClusterState state,
                                   ActionListener<SimulateIndexTemplateResponse> listener) throws Exception {
        String uuid = UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
        final String temporaryIndexName = "simulate_template_index_" + uuid;
        final ClusterState stateWithTemplate;
        final String simulateTemplateToAdd;

        // First, if a template body was requested, we need to "fake add" that template to the
        // cluster state, so it can be used when we resolved settings/etc
        if (request.getIndexTemplateRequest() != null) {
            // we'll "locally" add the template defined by the user in the cluster state (as if it
            // existed in the system), either with a temporary name, or with the given name if
            // specified, to simulate replacing the existing template
            simulateTemplateToAdd = request.getTemplateName() == null ? "simulate_template_" + uuid : request.getTemplateName();
            // Perform validation for things like typos in component template names
            MetadataIndexTemplateService.validateV2TemplateRequest(state.metadata(), simulateTemplateToAdd,
                request.getIndexTemplateRequest().indexTemplate());
            stateWithTemplate = indexTemplateService.addIndexTemplateV2(state, request.getIndexTemplateRequest().create(),
                simulateTemplateToAdd, request.getIndexTemplateRequest().indexTemplate());
        } else {
            simulateTemplateToAdd = null;
            stateWithTemplate = state;
        }

        // We also need the name of the template we're going to resolve, so if they specified a
        // name, use that, otherwise use the name of the template that was "fake added" in the previous block
        final String matchingTemplate;
        if (request.getTemplateName() == null) {
            // Automatically match the template that was added
            matchingTemplate = simulateTemplateToAdd;
        } else {
            matchingTemplate = request.getTemplateName();
        }

        // If they didn't either specify a name that existed or a template body, we cannot simulate anything!
        if (matchingTemplate == null) {
            // They should have specified either a template name or the body of a template, but neither were specified
            listener.onFailure(new IllegalArgumentException("a template name to match or a new template body must be specified"));
            return;
        } else if (stateWithTemplate.metadata().templatesV2().containsKey(matchingTemplate) == false) {
            // They specified a template, but it didn't exist
            listener.onFailure(new IllegalArgumentException("unable to simulate template [" + matchingTemplate + "] that does not exist"));
            return;
        }

        final ClusterState tempClusterState =
            TransportSimulateIndexTemplateAction.resolveTemporaryState(matchingTemplate, temporaryIndexName, stateWithTemplate);
        ComposableIndexTemplate templateV2 = tempClusterState.metadata().templatesV2().get(matchingTemplate);
        assert templateV2 != null : "the matched template must exist";

        Map<String, List<String>> overlapping = new HashMap<>();
        overlapping.putAll(findConflictingV1Templates(tempClusterState, matchingTemplate, templateV2.indexPatterns()));
        overlapping.putAll(findConflictingV2Templates(tempClusterState, matchingTemplate, templateV2.indexPatterns()));

        Template template = TransportSimulateIndexTemplateAction.resolveTemplate(matchingTemplate, temporaryIndexName,
            stateWithTemplate, xContentRegistry, indicesService, aliasValidator);
        listener.onResponse(new SimulateIndexTemplateResponse(template, overlapping));
    }

    @Override
    protected ClusterBlockException checkBlock(SimulateTemplateAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
