/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.cat;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.ResolvedRepositories;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * Cat API class to display information about snapshots
 */
@ServerlessScope(Scope.INTERNAL)
public class RestSnapshotAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_cat/snapshots"), new Route(GET, "/_cat/snapshots/{repository}"));
    }

    @Override
    public String getName() {
        return "cat_snapshot_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, NodeClient client) {
        final String[] matchAll = { ResolvedRepositories.ALL_PATTERN };
        GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest().repositories(request.paramAsStringArray("repository", matchAll))
            .snapshots(matchAll);

        getSnapshotsRequest.ignoreUnavailable(request.paramAsBoolean("ignore_unavailable", getSnapshotsRequest.ignoreUnavailable()));

        getSnapshotsRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getSnapshotsRequest.masterNodeTimeout()));

        return channel -> client.admin().cluster().getSnapshots(getSnapshotsRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(GetSnapshotsResponse getSnapshotsResponse) throws Exception {
                return RestTable.buildResponse(buildTable(request, getSnapshotsResponse), channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/snapshots/{repository}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        return new Table().startHeaders()
            .addCell("id", "alias:id,snapshot;desc:unique snapshot")
            .addCell("repository", "alias:re,repo;desc:repository name")
            .addCell("status", "alias:s,status;text-align:right;desc:snapshot name")
            .addCell("start_epoch", "alias:ste,startEpoch;desc:start time in seconds since 1970-01-01 00:00:00")
            .addCell("start_time", "alias:sti,startTime;desc:start time in HH:MM:SS")
            .addCell("end_epoch", "alias:ete,endEpoch;desc:end time in seconds since 1970-01-01 00:00:00")
            .addCell("end_time", "alias:eti,endTime;desc:end time in HH:MM:SS")
            .addCell("duration", "alias:dur,duration;text-align:right;desc:duration")
            .addCell("indices", "alias:i,indices;text-align:right;desc:number of indices")
            .addCell("successful_shards", "alias:ss,successful_shards;text-align:right;desc:number of successful shards")
            .addCell("failed_shards", "alias:fs,failed_shards;text-align:right;desc:number of failed shards")
            .addCell("total_shards", "alias:ts,total_shards;text-align:right;desc:number of total shards")
            .addCell("reason", "default:false;alias:r,reason;desc:reason for failures")
            .endHeaders();
    }

    private static final DateFormatter FORMATTER = DateFormatter.forPattern("HH:mm:ss").withZone(ZoneOffset.UTC);

    private Table buildTable(RestRequest req, GetSnapshotsResponse getSnapshotsResponse) {
        Table table = getTableWithHeader(req);

        if (getSnapshotsResponse.isFailed()) {
            ElasticsearchException causes = null;

            for (ElasticsearchException e : getSnapshotsResponse.getFailures().values()) {
                if (causes == null) {
                    causes = e;
                } else {
                    causes.addSuppressed(e);
                }
            }
            throw new ElasticsearchException(
                "Repositories ["
                    + Strings.collectionToCommaDelimitedString(getSnapshotsResponse.getFailures().keySet())
                    + "] failed to retrieve snapshots",
                causes
            );
        }

        for (SnapshotInfo snapshotStatus : getSnapshotsResponse.getSnapshots()) {
            table.startRow();

            table.addCell(snapshotStatus.snapshotId().getName());
            table.addCell(snapshotStatus.repository());
            table.addCell(snapshotStatus.state());
            table.addCell(TimeUnit.SECONDS.convert(snapshotStatus.startTime(), TimeUnit.MILLISECONDS));
            table.addCell(FORMATTER.format(Instant.ofEpochMilli(snapshotStatus.startTime())));
            table.addCell(TimeUnit.SECONDS.convert(snapshotStatus.endTime(), TimeUnit.MILLISECONDS));
            table.addCell(FORMATTER.format(Instant.ofEpochMilli(snapshotStatus.endTime())));
            final long durationMillis;
            if (snapshotStatus.state() == SnapshotState.IN_PROGRESS) {
                durationMillis = System.currentTimeMillis() - snapshotStatus.startTime();
            } else {
                durationMillis = snapshotStatus.endTime() - snapshotStatus.startTime();
            }
            table.addCell(TimeValue.timeValueMillis(durationMillis));
            table.addCell(snapshotStatus.indices().size());
            table.addCell(snapshotStatus.successfulShards());
            table.addCell(snapshotStatus.failedShards());
            table.addCell(snapshotStatus.totalShards());
            table.addCell(snapshotStatus.reason());

            table.endRow();
        }

        return table;
    }
}
