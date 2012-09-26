/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.unit.TimeValue.readTimeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A request to delete an index template.
 */
public class DeleteIndexTemplateRequest extends MasterNodeOperationRequest<DeleteIndexTemplateRequest> {

    private String name;

    private TimeValue timeout = timeValueSeconds(10);

    DeleteIndexTemplateRequest() {
    }

    /**
     * Constructs a new delete index request for the specified name.
     */
    public DeleteIndexTemplateRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        return validationException;
    }

    /**
     * The index template name to delete.
     */
    String name() {
        return name;
    }

    /**
     * Timeout to wait for the index deletion to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    TimeValue timeout() {
        return timeout;
    }

    /**
     * Timeout to wait for the index template deletion to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public DeleteIndexTemplateRequest timeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Timeout to wait for the index template deletion to be acknowledged by current cluster nodes. Defaults
     * to <tt>10s</tt>.
     */
    public DeleteIndexTemplateRequest timeout(String timeout) {
        return timeout(TimeValue.parseTimeValue(timeout, null));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        timeout = readTimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        timeout.writeTo(out);
    }
}