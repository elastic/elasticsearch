/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/// The priority of a task executed by the [MasterService], or by a [PrioritizedEsThreadPoolExecutor] (i.e. [ClusterApplierService]).
public enum Priority {

    /// The absolute highest priority level. Almost never used in practice. Only appropriate for tasks that may be needed to fix a situation
    /// in which the master is overwhelmed by other tasks with more sensible priorities, e.g. removing a node or performing a manual
    /// reroute.
    IMMEDIATE((byte) 0),

    /// The highest priority level in common use. Only appropriate for tasks that have an impact on service availability, such as adding
    /// nodes to the cluster or assigning unassigned shards, etc. It's usually a bad idea to let user-visible APIs directly trigger tasks at
    /// this level.
    URGENT((byte) 1),

    /// The next highest priority level in common use. Appropriate for tasks that have a strong need to complete ahead of the queue of
    /// [#NORMAL] tasks for some reason.
    HIGH((byte) 2),

    /// The usual priority level for most other tasks.
    NORMAL((byte) 3),

    /// The priority level for "background" tasks which we want to happen eventually, but we accept that it might take minutes to clear out
    /// the higher-priority queues.
    LOW((byte) 4),

    /// A sentinel priority level below [#LOW]. Never used for any actual tasks, only for `GET _cluster/health?wait_for_tasks=languid` to
    /// express a desire to wait for all tasks to complete.
    ///
    /// "Languid" is a real (although obscure) English word indicating a complete absence of urgency. You are not the first person who
    /// has wondered what this has to do with some combination of LANGuage + Unique ID.
    LANGUID((byte) 5);

    public static Priority readFrom(StreamInput input) throws IOException {
        return fromByte(input.readByte());
    }

    public static void writeTo(Priority priority, StreamOutput output) throws IOException {
        output.writeByte(priority.value);
    }

    public static Priority fromByte(byte b) {
        return switch (b) {
            case 0 -> IMMEDIATE;
            case 1 -> URGENT;
            case 2 -> HIGH;
            case 3 -> NORMAL;
            case 4 -> LOW;
            case 5 -> LANGUID;
            default -> throw new IllegalArgumentException("can't find priority for [" + b + "]");
        };
    }

    private final byte value;

    Priority(byte value) {
        this.value = value;
    }

    /**
     * @return whether tasks of {@code this} priority will run after those of priority {@code p}.
     *         For instance, {@code Priority.URGENT.after(Priority.IMMEDIATE)} returns {@code true}.
     */
    public boolean after(Priority p) {
        return this.compareTo(p) > 0;
    }

    /**
     * @return whether tasks of {@code this} priority will run no earlier than those of priority {@code p}.
     *         For instance, {@code Priority.URGENT.sameOrAfter(Priority.IMMEDIATE)} returns {@code true}.
     */
    public boolean sameOrAfter(Priority p) {
        return this.compareTo(p) >= 0;
    }

}
