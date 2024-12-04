/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.metering.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetBlobStoreStatsNodesResponse extends BaseNodesResponse<GetBlobStoreStatsNodeResponse> implements ToXContentFragment {
    public GetBlobStoreStatsNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    public GetBlobStoreStatsNodesResponse(
        ClusterName clusterName,
        List<GetBlobStoreStatsNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<GetBlobStoreStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readCollectionAsList(GetBlobStoreStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<GetBlobStoreStatsNodeResponse> nodes) throws IOException {
        out.writeCollection(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        final RepositoryStats aggregatedStats = getNodes().stream()
            .map(GetBlobStoreStatsNodeResponse::getRepositoryStats)
            .reduce(RepositoryStats::merge)
            .orElse(RepositoryStats.EMPTY_STATS);

        final RepositoryStats obsAggregatedStats = getNodes().stream()
            .map(GetBlobStoreStatsNodeResponse::getObsRepositoryStats)
            .reduce(RepositoryStats::merge)
            .orElse(RepositoryStats.EMPTY_STATS);

        builder.startObject("_all");
        {
            builder.startObject("object_store_stats");
            builder.field("request_counts", GetBlobStoreStatsNodeResponse.getRequestCounts(aggregatedStats));
            builder.endObject();
        }
        {
            builder.startObject("operational_backup_service_stats");
            builder.field("request_counts", GetBlobStoreStatsNodeResponse.getRequestCounts(obsAggregatedStats));
            builder.endObject();
        }
        builder.endObject();
        builder.startObject("nodes");
        for (GetBlobStoreStatsNodeResponse nodeStats : getNodes()) {
            nodeStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseNodesResponse<?> that = (BaseNodesResponse<?>) o;
        return Objects.equals(getClusterName(), that.getClusterName())
            && checkEqualityOfFailures(failures(), that.failures())
            && Objects.equals(getNodes(), that.getNodes());
    }

    private boolean checkEqualityOfFailures(List<FailedNodeException> thisFailures, List<FailedNodeException> thatFailures) {
        if (thisFailures == null && thatFailures == null) {
            return true;
        } else if (thisFailures == null || thatFailures == null) {
            return false;
        }
        if (thisFailures.size() != thatFailures.size()) return false;
        for (int i = 0; i < thisFailures.size(); i++) {
            FailedNodeException failureA = thisFailures.get(i);
            FailedNodeException failureB = thatFailures.get(i);
            if (failureA == failureB) return true;
            if (failureB == null || failureA.getClass() != failureB.getClass()) return false;
            if ((Objects.equals(failureA.nodeId(), failureB.nodeId()) && failureA.getMessage().equals(failureB.getMessage())
            // Could check elements individually, though there can be slight differences when serialized, so we just use the length.
                && failureA.getStackTrace().length == failureB.getStackTrace().length) == false) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        var failureMessages = failures().stream().map(Throwable::getMessage).toList();
        var failureClassNames = failures().stream().map(failure -> failure.getClass().getName()).toList();
        var failureStackTraceLengths = failures().stream().map(failure -> failure.getStackTrace().length).toList();
        return Objects.hash(
            getClusterName(),
            getNodes(),
            failureMessages,
            failureClassNames,
            failureStackTraceLengths // Could check elements individually, though there can be slight differences when serialized.
        );
    }
}
