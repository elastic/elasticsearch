/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestResponseListener;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestRecoveryAction provides information about the status of replica recovery
 * in a string format, designed to be used at the command line. An Index can
 * be specified to limit output to a particular index or indices.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestCatRecoveryAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/recovery"), new Route(GET, "/_cat/recovery/{index}"));
    }

    @Override
    public String getName() {
        return "cat_recovery_action";
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/recovery\n");
        sb.append("/_cat/recovery/{index}\n");
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final RecoveryRequest recoveryRequest = new RecoveryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        recoveryRequest.detailed(request.paramAsBoolean("detailed", false));
        recoveryRequest.activeOnly(request.paramAsBoolean("active_only", false));
        recoveryRequest.indicesOptions(IndicesOptions.fromRequest(request, recoveryRequest.indicesOptions()));

        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .recoveries(recoveryRequest, new RestResponseListener<RecoveryResponse>(channel) {
                @Override
                public RestResponse buildResponse(final RecoveryResponse response) throws Exception {
                    return RestTable.buildResponse(buildRecoveryTable(request, response), channel);
                }
            });
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table t = new Table();
        t.startHeaders()
            .addCell("index", "alias:i,idx;desc:index name")
            .addCell("shard", "alias:s,sh;desc:shard name")
            .addCell("start_time", "default:false;alias:start;desc:recovery start time")
            .addCell("start_time_millis", "default:false;alias:start_millis;desc:recovery start time in epoch milliseconds")
            .addCell("stop_time", "default:false;alias:stop;desc:recovery stop time")
            .addCell("stop_time_millis", "default:false;alias:stop_millis;desc:recovery stop time in epoch milliseconds")
            .addCell("time", "alias:t,ti;desc:recovery time")
            .addCell("type", "alias:ty;desc:recovery type")
            .addCell("stage", "alias:st;desc:recovery stage")
            .addCell("source_host", "alias:shost;desc:source host")
            .addCell("source_node", "alias:snode;desc:source node name")
            .addCell("target_host", "alias:thost;desc:target host")
            .addCell("target_node", "alias:tnode;desc:target node name")
            .addCell("repository", "alias:rep;desc:repository")
            .addCell("snapshot", "alias:snap;desc:snapshot")
            .addCell("files", "alias:f;desc:number of files to recover")
            .addCell("files_recovered", "alias:fr;desc:files recovered")
            .addCell("files_percent", "alias:fp;desc:percent of files recovered")
            .addCell("files_total", "alias:tf;desc:total number of files")
            .addCell("bytes", "alias:b;desc:number of bytes to recover")
            .addCell("bytes_recovered", "alias:br;desc:bytes recovered")
            .addCell("bytes_percent", "alias:bp;desc:percent of bytes recovered")
            .addCell("bytes_total", "alias:tb;desc:total number of bytes")
            .addCell("translog_ops", "alias:to;desc:number of translog ops to recover")
            .addCell("translog_ops_recovered", "alias:tor;desc:translog ops recovered")
            .addCell("translog_ops_percent", "alias:top;desc:percent of translog ops recovered")
            .endHeaders();
        return t;
    }

    /**
     * buildRecoveryTable will build a table of recovery information suitable
     * for displaying at the command line.
     *
     * @param request  A Rest request
     * @param response A recovery status response
     * @return A table containing index, shardId, node, target size, recovered size and percentage for each recovering replica
     */
    public Table buildRecoveryTable(RestRequest request, RecoveryResponse response) {

        Table t = getTableWithHeader(request);

        for (String index : response.shardRecoveryStates().keySet()) {

            List<RecoveryState> shardRecoveryStates = response.shardRecoveryStates().get(index);
            if (shardRecoveryStates.size() == 0) {
                continue;
            }

            // Sort ascending by shard id for readability
            CollectionUtil.introSort(shardRecoveryStates, Comparator.comparingInt(o -> o.getShardId().id()));

            for (RecoveryState state : shardRecoveryStates) {
                t.startRow();
                t.addCell(index);
                t.addCell(state.getShardId().id());
                t.addCell(XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(state.getTimer().startTime())));
                t.addCell(state.getTimer().startTime());
                t.addCell(XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(state.getTimer().stopTime())));
                t.addCell(state.getTimer().stopTime());
                t.addCell(new TimeValue(state.getTimer().time()));
                t.addCell(state.getRecoverySource().getType().toString().toLowerCase(Locale.ROOT));
                t.addCell(state.getStage().toString().toLowerCase(Locale.ROOT));
                t.addCell(state.getSourceNode() == null ? "n/a" : state.getSourceNode().getHostName());
                t.addCell(state.getSourceNode() == null ? "n/a" : state.getSourceNode().getName());
                t.addCell(state.getTargetNode().getHostName());
                t.addCell(state.getTargetNode().getName());
                t.addCell(
                    state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT
                        ? "n/a"
                        : ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getRepository()
                );
                t.addCell(
                    state.getRecoverySource() == null || state.getRecoverySource().getType() != RecoverySource.Type.SNAPSHOT
                        ? "n/a"
                        : ((SnapshotRecoverySource) state.getRecoverySource()).snapshot().getSnapshotId().getName()
                );
                t.addCell(state.getIndex().totalRecoverFiles());
                t.addCell(state.getIndex().recoveredFileCount());
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredFilesPercent()));
                t.addCell(state.getIndex().totalFileCount());
                t.addCell(ByteSizeValue.ofBytes(state.getIndex().totalRecoverBytes()));
                t.addCell(ByteSizeValue.ofBytes(state.getIndex().recoveredBytes()));
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredBytesPercent()));
                t.addCell(ByteSizeValue.ofBytes(state.getIndex().totalBytes()));
                t.addCell(state.getTranslog().totalOperations());
                t.addCell(state.getTranslog().recoveredOperations());
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getTranslog().recoveredPercent()));
                t.endRow();
            }
        }

        return t;
    }
}
