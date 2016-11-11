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

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class VerificationFailure implements Streamable {

    private String nodeId;

    private Exception cause;

    VerificationFailure() {

    }

    public VerificationFailure(String nodeId, Exception cause) {
        this.nodeId = nodeId;
        this.cause = cause;
    }

    public String nodeId() {
        return nodeId;
    }

    public Throwable cause() {
        return cause;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        nodeId = in.readOptionalString();
        cause = in.readException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(nodeId);
        out.writeException(cause);
    }

    public static VerificationFailure readNode(StreamInput in) throws IOException {
        VerificationFailure failure = new VerificationFailure();
        failure.readFrom(in);
        return failure;
    }

    @Override
    public String toString() {
        return "[" + nodeId + ", '" + cause + "']";
    }
}
