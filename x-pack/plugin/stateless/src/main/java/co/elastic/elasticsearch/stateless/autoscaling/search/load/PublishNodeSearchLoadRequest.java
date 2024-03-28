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

package co.elastic.elasticsearch.stateless.autoscaling.search.load;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class PublishNodeSearchLoadRequest extends MasterNodeRequest<PublishNodeSearchLoadRequest> {

    private final String nodeId;
    private final long seqNo;
    private final double searchLoad;

    public PublishNodeSearchLoadRequest(String nodeId, long seqNo, double searchLoad) {
        super();
        this.nodeId = nodeId;
        this.seqNo = seqNo;
        this.searchLoad = searchLoad;
    }

    public PublishNodeSearchLoadRequest(StreamInput in) throws IOException {
        super(in);
        this.nodeId = in.readString();
        this.seqNo = in.readLong();
        this.searchLoad = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(nodeId);
        out.writeLong(seqNo);
        out.writeDouble(searchLoad);
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public double getSearchLoad() {
        return searchLoad;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PublishNodeSearchLoadRequest that = (PublishNodeSearchLoadRequest) o;
        return seqNo == that.seqNo && Double.compare(that.searchLoad, searchLoad) == 0 && Objects.equals(nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, seqNo, searchLoad);
    }

    @Override
    public String toString() {
        return "PublishNodeSearchLoadRequest{nodeId='" + nodeId + "', seqNo=" + seqNo + ", searchLoad=" + searchLoad + '}';
    }
}
