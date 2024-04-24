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

package co.elastic.elasticsearch.stateless.cache.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.action.support.nodes.BaseNodesXContentResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class ClearBlobCacheNodesResponse extends BaseNodesXContentResponse<ClearBlobCacheNodeResponse> implements ChunkedToXContentObject {

    public ClearBlobCacheNodesResponse(
        ClusterName clusterName,
        List<ClearBlobCacheNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<ClearBlobCacheNodeResponse> readNodesFrom(StreamInput in) {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<ClearBlobCacheNodeResponse> nodes) {
        TransportAction.localOnly();
    }

    @Override
    protected Iterator<? extends ToXContent> xContentChunks(ToXContent.Params outerParams) {
        return Iterators.concat(
            ChunkedToXContentHelper.startArray("nodes"),
            Iterators.flatMap(
                getNodes().iterator(),
                clearBlobCacheNodeResponse -> Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    clearBlobCacheNodeResponse.toXContentChunked(outerParams),
                    ChunkedToXContentHelper.endObject()
                )
            ),
            ChunkedToXContentHelper.endArray()
        );
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
