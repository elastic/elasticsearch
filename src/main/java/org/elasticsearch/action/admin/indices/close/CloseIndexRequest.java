/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.unit.TimeValue.readTimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A request to close an index.
 */
public class CloseIndexRequest extends MasterNodeOperationRequest<CloseIndexRequest> {

    private String[] indices;
    private TimeValue timeout = timeValueSeconds(10);
    private IgnoreIndices ignoreIndices = IgnoreIndices.DEFAULT;

    CloseIndexRequest() {
    }

    /**
     * Constructs a new close index request for the specified index.
     */
    public CloseIndexRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null || indices.length == 0) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    /**
     * The indices to be closed
     * @return the indices to be closed
     */
    String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be closed
     * @param indices the indices to be closed
     * @return the request itself
     */
    public CloseIndexRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Timeout to wait for the index closure to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    TimeValue timeout() {
        return timeout;
    }

    /**
     * Timeout to wait for the index closure to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public CloseIndexRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Timeout to wait for the index closure to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public CloseIndexRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null));
    }


    /**
     * Specifies what type of requested indices to ignore. For example indices that don't exist.
     * @return the desired behaviour regarding indices to ignore
     */
    public IgnoreIndices ignoreIndices() {
        return ignoreIndices;
    }

    /**
     * Specifies what type of requested indices to ignore. For example indices that don't exist.
     * @param ignoreIndices the desired behaviour regarding indices to ignore
     * @return the request itself
     */
    public CloseIndexRequest ignoreIndices(IgnoreIndices ignoreIndices) {
        this.ignoreIndices = ignoreIndices;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        timeout = readTimeValue(in);
        ignoreIndices = IgnoreIndices.fromId(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        timeout.writeTo(out);
        out.writeByte(ignoreIndices.id());
    }
}
