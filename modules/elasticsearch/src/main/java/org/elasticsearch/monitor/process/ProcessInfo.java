/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.monitor.process;

import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author kimchy (shay.banon)
 */
public class ProcessInfo implements Streamable, Serializable {

    private long id;

    private ProcessInfo() {

    }

    public ProcessInfo(long id) {
        this.id = id;
    }

    /**
     * The process id.
     */
    public long id() {
        return this.id;
    }

    /**
     * The process id.
     */
    public long getId() {
        return id();
    }

    public static ProcessInfo readProcessInfo(StreamInput in) throws IOException {
        ProcessInfo info = new ProcessInfo();
        info.readFrom(in);
        return info;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        id = in.readLong();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(id);
    }
}
