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

import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.recovery.ShardRecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.RecoveryMetrics;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestTable;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * RestRecoveryAction provides information about the status of replica recovery
 * in a string format, designed to be used at the command line. An Index can
 * be specified to limit output to a particular index or indices.
 */
public class RestRecoveryAction extends AbstractCatAction {

    private static final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateHourMinuteSecond();

    @Inject
    protected RestRecoveryAction(Settings settings, Client client, RestController restController) {
        super(settings, client);
        restController.registerHandler(GET, "/_cat/recovery", this);
        restController.registerHandler(GET, "/_cat/recovery/{index}", this);
    }

    @Override
    void documentation(StringBuilder sb) {
        sb.append("/_cat/recovery\n");
        sb.append("/_cat/recovery/{index}\n");
    }

    @Override
    public void doRequest(final RestRequest request, final RestChannel channel) {

        final RecoveryRequest recoveryRequest = new RecoveryRequest(Strings.splitStringByCommaToArray(request.param("index")));
        recoveryRequest.detailed(request.paramAsBoolean("details", false));
        recoveryRequest.listenerThreaded(false);
        recoveryRequest.indicesOptions(IndicesOptions.fromRequest(request, recoveryRequest.indicesOptions()));

        client.admin().indices().recoveries(recoveryRequest, new ActionListener<RecoveryResponse>() {

            @Override
            public void onResponse(final RecoveryResponse response) {
                try {
                    channel.sendResponse(RestTable.buildResponse(buildRecoveryTable(request, response), request, channel));
                } catch (Throwable e) {
                    try {
                        channel.sendResponse(new XContentThrowableRestResponse(request, e));
                    } catch (IOException e2) {
                        logger.error("Unable to send recovery status response", e2);
                    }
                }
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    channel.sendResponse(new XContentThrowableRestResponse(request, e));
                } catch (IOException e1) {
                    logger.error("Failed to send failure response", e1);
                }
            }
        });
    }

    @Override
    Table getTableWithHeader(RestRequest request) {
        Table t = new Table();
        t.startHeaders()
                .addCell("index", "alias:i,idx;desc:index name")
                .addCell("shard", "alias:s,sh;desc:shard name")
                .addCell("start", "alias:sta;desc:start time")
                .addCell("stop", "alias:sto;desc:stop time")
                .addCell("elapsed", "alias:t,ti;desc:recovery time")
                .addCell("type", "alias:ty;desc:recovery type")
                .addCell("stage", "alias:st;desc:recovery stage")
                .addCell("files", "alias:f;desc:number of files")
                .addCell("files_recovered", "alias:fr;desc:number of files recovered")
                .addCell("files_percent", "alias:fp;desc:percent of files recovered")
                .addCell("bytes", "alias:b;desc:size in bytes")
                .addCell("bytes_recovered", "alias:br;desc:bytes recovered")
                .addCell("bytes_percent", "alias:bp;desc:percent of bytes recovered")
                .addCell("source_host", "alias:th;desc:source host")
                .addCell("target_host", "alias:th;desc:target host")
                .addCell("repository", "alias:rep;desc:repository")
                .addCell("snapshot", "alias:snap;desc:snapshot")
                .endHeaders();
        return t;
    }

    public Table buildRecoveryTable(RestRequest request, RecoveryResponse response) {

        Table t = getTableWithHeader(request);
        for (String index : response.shardResponses().keySet()) {

            List<ShardRecoveryResponse> shardRecoveryResponses = response.shardResponses().get(index);
            for (ShardRecoveryResponse shardRecoveryResponse : shardRecoveryResponses) {

                RecoveryMetrics metrics = shardRecoveryResponse.recoveryMetrics();
                boolean isSnapshotRestore = metrics.type() == RecoveryMetrics.Type.RESTORE;

                t.startRow();
                t.addCell(index);
                t.addCell(shardRecoveryResponse.getShardId());
                t.addCell(metrics.timer().startTime() > 0 ? dateFormatter.print(metrics.timer().startTime()) : 0);
                t.addCell(metrics.timer().stopTime() > 0 ? dateFormatter.print(metrics.timer().stopTime()) : 0);
                t.addCell(metrics.timer().elapsed());
                t.addCell(metrics.type().toString().toLowerCase(Locale.ROOT));
                t.addCell(metrics.state().toString().toLowerCase(Locale.ROOT));
                t.addCell(metrics.index().numberOfFiles());
                t.addCell(metrics.index().numberOfFilesRecovered());
                t.addCell(safePercent(metrics.index().numberOfFilesRecovered(), metrics.index().numberOfFiles()));
                t.addCell(metrics.index().numberOfBytes());
                t.addCell(metrics.index().numberOfBytesRecovered());
                t.addCell(safePercent(metrics.index().numberOfBytesRecovered(), metrics.index().numberOfBytes()));
                t.addCell(isSnapshotRestore ? "n/a" : metrics.sourceNode().getHostName());
                t.addCell(metrics.targetNode().getHostName());
                t.addCell(isSnapshotRestore ? metrics.restoreSource().snapshotId().getRepository() : "n/a");
                t.addCell(isSnapshotRestore ? metrics.restoreSource().snapshotId().getSnapshot() : "n/a");
                t.endRow();
            }
        }
        return t;
    }

    private String safePercent(long a, long b) {
        if (b == 0) {
            return String.format(Locale.ROOT, "%1.1f%%", 0.0f);
        } else {
            return String.format(Locale.ROOT, "%1.1f%%", 100.0 * (float) a / b);
        }
    }
}
