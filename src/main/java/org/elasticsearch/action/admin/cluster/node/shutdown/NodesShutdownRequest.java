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

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

import static org.elasticsearch.common.unit.TimeValue.readTimeValue;

/**
 *
 */
public class NodesShutdownRequest extends MasterNodeOperationRequest<NodesShutdownRequest> {

    String[] nodesIds = Strings.EMPTY_ARRAY;

    TimeValue delay = TimeValue.timeValueSeconds(1);

    boolean exit = true;

    NodesShutdownRequest() {
    }

    public NodesShutdownRequest(String... nodesIds) {
        this.nodesIds = nodesIds;
    }

    public NodesShutdownRequest nodesIds(String... nodesIds) {
        this.nodesIds = nodesIds;
        return this;
    }

    /**
     * The delay for the shutdown to occur. Defaults to <tt>1s</tt>.
     */
    public NodesShutdownRequest delay(TimeValue delay) {
        this.delay = delay;
        return this;
    }

    public TimeValue delay() {
        return this.delay;
    }

    /**
     * The delay for the shutdown to occur. Defaults to <tt>1s</tt>.
     */
    public NodesShutdownRequest delay(String delay) {
        return delay(TimeValue.parseTimeValue(delay, null));
    }

    /**
     * Should the JVM be exited as well or not. Defaults to <tt>true</tt>.
     */
    public NodesShutdownRequest exit(boolean exit) {
        this.exit = exit;
        return this;
    }

    /**
     * Should the JVM be exited as well or not. Defaults to <tt>true</tt>.
     */
    public boolean exit() {
        return exit;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        delay = readTimeValue(in);
        nodesIds = in.readStringArray();
        exit = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        delay.writeTo(out);
        out.writeStringArrayNullable(nodesIds);
        out.writeBoolean(exit);
    }
}
