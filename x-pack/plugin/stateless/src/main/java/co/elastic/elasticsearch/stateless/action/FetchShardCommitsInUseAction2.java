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

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * {@link TransportFetchShardCommitsInUseAction} broadcasts a request to a set of search nodes asking what commits are still in use for a
 * particular shard.
 *
 * {@link FetchShardCommitsInUseAction.Request} is the request made by the index node to the transport layer, and
 * {@link FetchShardCommitsInUseAction.Response} is the response returned. {@link FetchShardCommitsInUseAction.NodeRequest} and
 * {@link FetchShardCommitsInUseAction.NodeResponse} are the individual search node requests and responses handled in the transport layer.
 */
public class FetchShardCommitsInUseAction {
    private static final Logger logger = LogManager.getLogger(FetchShardCommitsInUseAction.class);

    public static class Request extends BaseNodesRequest {
        private final ShardId shardId;

        /**
         * @param nodesIds The IDs of the nodes to ask for commits-in-use information.
         * @param shardId The shardId to identify the particular shard in the index.
         */
        public Request(String[] nodesIds, ShardId shardId) {
            super(nodesIds);
            this.shardId = shardId;
        }

        public ShardId getShardId() {
            return this.shardId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, nodesIds());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            FetchShardCommitsInUseAction.Request that = (FetchShardCommitsInUseAction.Request) obj;
            return Objects.equals(this.shardId, that.shardId) && Arrays.equals(this.nodesIds(), that.nodesIds());
        }
    }

    /**
     * Request object for fetching which commits the search shard copy on a node is actively using for reads.
     */
    public static class NodeRequest extends ActionRequest {
        private final ShardId shardId;

        /**
         * @param request The original index node request handed to the transport layer, which is to be broadcast to individual nodes.
         */
        public NodeRequest(Request request) {
            super();
            this.shardId = request.getShardId();
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
        }

        public ShardId getShardId() {
            return this.shardId;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeWriteable(this.shardId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return super.toString() + ", shardId: " + this.shardId;
        }
    }

    /**
     * The combined response containing the combined list of commits-in-use.
     * Contains information about which serverless shard commits are still in active use by which search nodes.
     */
    public static class Response extends BaseNodesResponse<FetchShardCommitsInUseAction.NodeResponse> {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(
            ClusterName clusterName,
            List<FetchShardCommitsInUseAction.NodeResponse> nodeResponses,
            List<FailedNodeException> nodeFailures
        ) {
            super(clusterName, nodeResponses, nodeFailures);
        }

        /**
         * Combines the commits-in-use from each node that was called into a single set for the final {@link Response}.
         */
        public Set<PrimaryTermAndGeneration> getAllPrimaryTermAndGenerationsInUse() {
            Set<PrimaryTermAndGeneration> allPrimaryTermAndGenerationsInUse = new HashSet<>();
            for (FetchShardCommitsInUseAction.NodeResponse nodeResponse : getNodes()) {
                allPrimaryTermAndGenerationsInUse.addAll(nodeResponse.primaryTermAndGenerationsInUse);
            }
            return allPrimaryTermAndGenerationsInUse;
        }

        /**
         * Serializes the list of {@link NodeResponse} instances into an output stream.
         */
        @Override
        protected void writeNodesTo(StreamOutput out, List<FetchShardCommitsInUseAction.NodeResponse> nodeResponses) throws IOException {
            out.writeCollection(nodeResponses);
        }

        /**
         * Deserializes the node response instances from the input stream into a list of {@link NodeResponse}.
         */
        @Override
        protected List<FetchShardCommitsInUseAction.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(FetchShardCommitsInUseAction.NodeResponse::new);
        }

        @Override
        public String toString() {
            return "FetchShardCommitsInUseAction.Response{" + "FetchShardCommitsInUseAction.NodeResponse=" + getNodes() + "}";
        }
    }

    /**
     * The response of a single node receiving a FetchShardCommitsInUseAction transport request.
     * Holds a list of serverless commits ({@link PrimaryTermAndGeneration}) actively in use by the search shard on that node.
     */
    public static class NodeResponse extends BaseNodeResponse {
        private Set<PrimaryTermAndGeneration> primaryTermAndGenerationsInUse;

        public NodeResponse(DiscoveryNode node, Set<PrimaryTermAndGeneration> primaryTermAndGenerationsInUse) {
            super(node);
            this.primaryTermAndGenerationsInUse = primaryTermAndGenerationsInUse;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.primaryTermAndGenerationsInUse = in.readCollectionAsSet(PrimaryTermAndGeneration::new);
        }

        public Set<PrimaryTermAndGeneration> getPrimaryTermAndGenerationsInUse() {
            return primaryTermAndGenerationsInUse;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(primaryTermAndGenerationsInUse);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FetchShardCommitsInUseAction.NodeResponse that = (FetchShardCommitsInUseAction.NodeResponse) o;
            return Objects.equals(this.getNode(), that.getNode())
                && Objects.equals(this.primaryTermAndGenerationsInUse, that.primaryTermAndGenerationsInUse);
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryTermAndGenerationsInUse, getNode());
        }

        @Override
        public String toString() {
            return "FetchShardCommitsInUseAction.NodeResponse{"
                + "nodeId="
                + getNode().getId()
                + ", primaryTermAndGenerationsInUse="
                + primaryTermAndGenerationsInUse
                + "}";
        }
    }
}
