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

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 */
public class PendingClusterTask implements Streamable {

    private long insertOrder;
    private Priority priority;
    private Text source;
    private long timeInQueue;

    public PendingClusterTask() {
    }

    public PendingClusterTask(long insertOrder, Priority priority, Text source, long timeInQueue) {
        this.insertOrder = insertOrder;
        this.priority = priority;
        this.source = source;
        this.timeInQueue = timeInQueue;
    }

    public long insertOrder() {
        return insertOrder;
    }

    public long getInsertOrder() {
        return insertOrder();
    }

    public Priority priority() {
        return priority;
    }

    public Priority getPriority() {
        return priority();
    }

    public Text source() {
        return source;
    }

    public Text getSource() {
        return source();
    }

    public long timeInQueueInMillis() {
        return timeInQueue;
    }

    public long getTimeInQueueInMillis() {
        return timeInQueueInMillis();
    }

    public TimeValue getTimeInQueue() {
        return new TimeValue(getTimeInQueueInMillis());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        insertOrder = in.readVLong();
        priority = Priority.fromByte(in.readByte());
        source = in.readText();
        timeInQueue = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(insertOrder);
        out.writeByte(priority.value());
        out.writeText(source);
        out.writeVLong(timeInQueue);
    }
}