/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestCatIndexLifecycleAction extends AbstractCatAction {
    /**
     * options:
     * - h, s, v, help
     * - expand_wildcards
     * - health
     * - master_timeout
     * @param request
     * @param client
     * @return
     */
    @Override
    protected RestChannelConsumer doCatRequest(RestRequest request, NodeClient client) {

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.strictExpandOpen());
        TimeValue masterNodeTimeout = getMasterNodeTimeout(request);
        boolean onlyManaged = request.paramAsBoolean("only_managed", false);
        boolean onlyErrors = request.paramAsBoolean("only_errors", false);

        // explain ilm request
        ExplainLifecycleRequest explainLifecycleRequest = new ExplainLifecycleRequest();
        explainLifecycleRequest.indices(indices);
        explainLifecycleRequest.indicesOptions(indicesOptions);
        explainLifecycleRequest.onlyManaged(onlyManaged);
        explainLifecycleRequest.onlyErrors(onlyErrors);
        explainLifecycleRequest.masterNodeTimeout(masterNodeTimeout);

        return channel -> client.execute(ExplainLifecycleAction.INSTANCE, explainLifecycleRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(ExplainLifecycleResponse response) throws Exception {
                Map<String, IndexLifecycleExplainResponse> indexResponses = response.getIndexResponses();

                Table table = getTableWithHeader(request);

                indexResponses.forEach((indexName, indexLifecycle) -> {
                    table.startRow();

                    table.addCell(indexLifecycle.getIndex());
                    table.addCell(indexLifecycle.managedByILM());
                    table.addCell(indexLifecycle.getPolicyName());
                    table.addCell(indexLifecycle.getPhase());
                    table.addCell(indexLifecycle.getAction());
                    table.addCell(indexLifecycle.getStep());

                    table.addCell(indexLifecycle.getFailedStep());
                    table.addCell(indexLifecycle.isAutoRetryableError());
                    table.addCell(indexLifecycle.getFailedStepRetryCount());

                    table.endRow();
                });

                return RestTable.buildResponse(table, channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/ilm/indices\n");
        sb.append("/_cat/ilm/indices/{index}\n");
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        Table table = new Table();
        table.startHeaders();

        table.addCell("index", "alias:i,idx;desc:index name");
        table.addCell("ilm_managed", "alias:managed;desc:if index is managed by ilm");
        table.addCell("policy", "alias:p;desc:ilm policy");
        table.addCell("phase", "alias:ph;desc:current ilm phase");
        table.addCell("action", "alias:ac;desc:current ilm action");
        table.addCell("step", "alias:st;desc:current ilm step");

        table.addCell("failed_step", "alias:fs;default:false;desc:if error occur, the step that caused the it");
        table.addCell("retryable", "alias:rty;default:false;desc:if retrying the failed step can overcome the error. If this is true, ILM will retry the failed step automatically");
        table.addCell("retry_count", "alias:rc;default:false;desc:number of attempted automatic retries");
        table.addCell("failed_step.type", "alias:fst;default:false;desc:type of failure");

        table.endHeaders();
        return table;
    }

    @Override
    public String getName() {
        return "cat_ilm_indices_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/ilm/indices"), new Route(GET, "/_cat/ilm/indices/{index}"));
    }
}
