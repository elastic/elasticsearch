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

package org.elasticsearch.rest.action.cat;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestResponseListener;
import org.elasticsearch.rest.action.support.RestTable;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestRecoveryAction provides information about the status of replica recovery
 * in a string format, designed to be used at the command line. An Index can
 * be specified to limit output to a particular index or indices.
 */
public class RestRecoveryAction extends AbstractCatAction {

    @Inject
    protected RestRecoveryAction(Settings settings, RestController restController, RestController controller, Client client) {
        super(settings, controller, client);
        restController.registerHandler(GET, "/_cat/recovery", this);
        restController.registerHandler(GET, "/_cat/recovery/{index}", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/recovery\n");
        sb.append("/_cat/recovery/{index}\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final RecoveryRequest recoveryRequest = new RecoveryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        recoveryRequest.detailed(request.paramAsBoolean("detailed", false));
        recoveryRequest.activeOnly(request.paramAsBoolean("active_only", false));
        recoveryRequest.listenerThreaded(false);
        recoveryRequest.indicesOptions(IndicesOptions.fromRequest(request, recoveryRequest.indicesOptions()));

        client.admin().indices().recoveries(recoveryRequest, new RestResponseListener<RecoveryResponse>(channel) {
            @Override
            public RestResponse buildResponse(final RecoveryResponse response) throws Exception {
                return RestTable.buildResponse(buildRecoveryTable(request, response), channel);
            }
        });
    }

    @Override
    Table getTableWithHeader(RestRequest request) {
        Table t = new Table();
        t.startHeaders()
                .addCell("index", "alias:i,idx;desc:index name")
                .addCell("shard", "alias:s,sh;desc:shard name")
                .addCell("time", "alias:t,ti;desc:recovery time")
                .addCell("type", "alias:ty;desc:recovery type")
                .addCell("stage", "alias:st;desc:recovery stage")
                .addCell("source_host", "alias:shost;desc:source host")
                .addCell("target_host", "alias:thost;desc:target host")
                .addCell("repository", "alias:rep;desc:repository")
                .addCell("snapshot", "alias:snap;desc:snapshot")
                .addCell("files", "alias:f;desc:number of files to recover")
                .addCell("files_percent", "alias:fp;desc:percent of files recovered")
                .addCell("bytes", "alias:b;desc:size to recover in bytes")
                .addCell("bytes_percent", "alias:bp;desc:percent of bytes recovered")
                .addCell("total_files", "alias:tf;desc:total number of files")
                .addCell("total_bytes", "alias:tb;desc:total number of bytes")
                .addCell("translog", "alias:tr;desc:translog operations recovered")
                .addCell("translog_percent", "alias:trp;desc:percent of translog recovery")
                .addCell("total_translog", "alias:trt;desc:current total translog operations")
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

        for (String index : response.shardResponses().keySet()) {

            List<ShardRecoveryResponse> shardRecoveryResponses = response.shardResponses().get(index);
            if (shardRecoveryResponses.size() == 0) {
                continue;
            }

            // Sort ascending by shard id for readability
            CollectionUtil.introSort(shardRecoveryResponses, new Comparator<ShardRecoveryResponse>() {
                @Override
                public int compare(ShardRecoveryResponse o1, ShardRecoveryResponse o2) {
                    int id1 = o1.recoveryState().getShardId().id();
                    int id2 = o2.recoveryState().getShardId().id();
                    if (id1 < id2) {
                        return -1;
                    } else if (id1 > id2) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            for (ShardRecoveryResponse shardResponse : shardRecoveryResponses) {

                RecoveryState state = shardResponse.recoveryState();
                t.startRow();
                t.addCell(index);
                t.addCell(shardResponse.getShardId());
                t.addCell(state.getTimer().time());
                t.addCell(state.getType().toString().toLowerCase(Locale.ROOT));
                t.addCell(state.getStage().toString().toLowerCase(Locale.ROOT));
                t.addCell(state.getSourceNode() == null ? "n/a" : state.getSourceNode().getHostName());
                t.addCell(state.getTargetNode().getHostName());
                t.addCell(state.getRestoreSource() == null ? "n/a" : state.getRestoreSource().snapshotId().getRepository());
                t.addCell(state.getRestoreSource() == null ? "n/a" : state.getRestoreSource().snapshotId().getSnapshot());
                t.addCell(state.getIndex().totalRecoverFiles());
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredFilesPercent()));
                t.addCell(state.getIndex().totalRecoverBytes());
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getIndex().recoveredBytesPercent()));
                t.addCell(state.getIndex().totalFileCount());
                t.addCell(state.getIndex().totalBytes());
                t.addCell(state.getTranslog().recoveredOperations());
                t.addCell(String.format(Locale.ROOT, "%1.1f%%", state.getTranslog().recoveredPercent()));
                t.addCell(state.getTranslog().totalOperations());
                t.endRow();
            }
        }

        return t;
    }
}
