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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class PublishNodeIngestLoadRequest extends MasterNodeRequest<PublishNodeIngestLoadRequest> {

    private final String nodeId;
    private final String nodeName;
    private final long seqNo;
    private final double ingestionLoad;

    public PublishNodeIngestLoadRequest(String nodeId, String nodeName, long seqNo, double ingestionLoad) {
        super(TimeValue.MINUS_ONE);
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.seqNo = seqNo;
        this.ingestionLoad = ingestionLoad;
    }

    public PublishNodeIngestLoadRequest(StreamInput in) throws IOException {
        super(in);
        this.nodeId = in.readString();
        // TODO: Remove version BWC once all nodes are on newer version
        if (in.getTransportVersion().onOrAfter(ServerlessTransportVersions.NODE_NAME_IN_PUBLISH_INGEST_LOAD_REQUEST)) {
            this.nodeName = in.readString();
        } else {
            this.nodeName = "";
        }
        this.seqNo = in.readLong();
        this.ingestionLoad = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        // TODO: Remove version BWC once all nodes are on newer version
        if (out.getTransportVersion().onOrAfter(ServerlessTransportVersions.NODE_NAME_IN_PUBLISH_INGEST_LOAD_REQUEST)) {
            out.writeString(nodeName);
        }
        out.writeLong(seqNo);
        out.writeDouble(ingestionLoad);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getNodeName() {
        return nodeName;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public double getIngestionLoad() {
        return ingestionLoad;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PublishNodeIngestLoadRequest that = (PublishNodeIngestLoadRequest) o;
        return seqNo == that.seqNo
            && Double.compare(that.ingestionLoad, ingestionLoad) == 0
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(nodeName, that.nodeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, seqNo, ingestionLoad, nodeName);
    }

    @Override
    public String toString() {
        return "PublishNodeIngestLoadRequest{nodeId='"
            + nodeId
            + "', nodeName='"
            + nodeName
            + "', seqNo="
            + seqNo
            + ", ingestionLoad="
            + ingestionLoad
            + '}';
    }
}
