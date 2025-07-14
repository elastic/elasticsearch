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

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to reshard an index.
 */
public class ReshardIndexRequest extends MasterNodeRequest<ReshardIndexRequest> implements IndicesRequest {
    private final String index;
    private final int multiple;

    public ReshardIndexRequest(StreamInput in) throws IOException {
        super(in);
        index = in.readString();
        multiple = in.readInt();
    }

    public ReshardIndexRequest(String index, int multiple) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT);
        this.index = index;
        this.multiple = multiple;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(new String[] { index })) {
            validationException = addValidationError("index is missing", validationException);
        }
        if (multiple <= 1) {
            validationException = addValidationError("multiple must be greater than 1", validationException);
        }
        return validationException;
    }

    @Override
    public String[] indices() {
        return new String[] { index };
    }

    /**
     * Name of index we intend to autoshard.
     */
    public String index() {
        return index;
    }

    /**
     * The factor by which to multiply the current shard count
     */
    public int multiple() {
        return multiple;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeInt(multiple);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ReshardIndexRequest that = (ReshardIndexRequest) obj;
        return Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
}
