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

package org.elasticsearch.protocol.xpack.indexlifecycle;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class PutOperationModeRequest extends AcknowledgedRequest<PutOperationModeRequest> {

    private OperationMode mode;

    public PutOperationModeRequest(OperationMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("mode cannot be null");
        }
        if (mode == OperationMode.STOPPED) {
            throw new IllegalArgumentException("cannot directly stop index-lifecycle");
        }
        this.mode = mode;
    }

    public PutOperationModeRequest() {
    }

    public OperationMode getMode() {
        return mode;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        mode = in.readEnum(OperationMode.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(mode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        PutOperationModeRequest other = (PutOperationModeRequest) obj;
        return Objects.equals(mode, other.mode);
    }
}