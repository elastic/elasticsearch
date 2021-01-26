/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a unit of execution of a task on the current node or a remote node
 */
public final class TaskSpan {
    public static final String REMOTE_NODE = "remote_node";
    public static final String REMOTE_ACTION = "remote_action";
    public static final String SHARD_ID = "shard_id";
    public static final String THREAD_NAME = "thread_name";

    private final long startTimeInMillis;
    private long endTimeInMillis = -1L;
    private Exception failure = null;
    private Map<String, Object> attributes;
    private List<TaskSpan> childSpans;

    public TaskSpan() {
        this.startTimeInMillis = System.currentTimeMillis();
    }

    synchronized TaskSpanInfo getSpanInfo() {
        final List<TaskSpanInfo> childInfos;
        if (childSpans == null) {
            childInfos = List.of();
        } else {
            childInfos = childSpans.stream().map(TaskSpan::getSpanInfo).collect(Collectors.toList());
        }
        final Map<String, Object> serializableAttributes;
        if (attributes != null) {
            serializableAttributes = new HashMap<>(attributes.size());
            for (Map.Entry<String, Object> e : attributes.entrySet()) {
                if (e.getValue() instanceof WeakReference) {
                    final Object v = ((WeakReference<?>) e.getValue()).get();
                    if (v != null) {
                        final String str;
                        if (v instanceof TaskAwareRequest) {
                            str = ((TaskAwareRequest) v).getDescription();
                        } else {
                            str = v.toString();
                        }
                        serializableAttributes.put(e.getKey(), str);
                    }
                } else {
                    serializableAttributes.put(e.getKey(), e.getValue());
                }
            }
        } else {
            serializableAttributes = Map.of();
        }
        final long runningTime = (endTimeInMillis != -1L ? endTimeInMillis : System.currentTimeMillis()) - startTimeInMillis;
        return new TaskSpanInfo(startTimeInMillis, runningTime, isDone(), failure, serializableAttributes, childInfos);
    }

    public synchronized TaskSpan newChildSpan() {
        if (isDone()) {
            throw new IllegalStateException("Task span is completed already");
        }
        if (childSpans == null) {
            childSpans = new ArrayList<>();
        } else {
            int size = childSpans.size();
            final Iterator<TaskSpan> it = childSpans.iterator();
            while (size > 1000 && it.hasNext()) {
                final TaskSpan span = it.next();
                if (span.isDone()) {
                    it.remove();
                    --size;
                }
            }
        }
        final TaskSpan sub = new TaskSpan();
        childSpans.add(sub);
        return sub;
    }

    public TaskSpan addAttribute(String tag, String value) {
        return innerAddAttribute(tag, value);
    }

    public TaskSpan addAttribute(String tag, boolean value) {
        return innerAddAttribute(tag, value);
    }

    public TaskSpan addAttribute(String tag, int value) {
        return innerAddAttribute(tag, value);
    }

    public TaskSpan addAttribute(String tag, Object value) {
        return innerAddAttribute(tag, new WeakReference<>(value));
    }

    public synchronized void removeAttribute(String tag) {
        if (attributes != null) {
            attributes.remove(tag);
        }
    }

    private synchronized TaskSpan innerAddAttribute(String tag, Object value) {
        if (attributes == null) {
            attributes = new HashMap<>();
        }
        attributes.put(tag, value);
        return this;
    }

    public synchronized Object getAttribute(String tag) {
        return attributes.get(tag);
    }

    public synchronized void markAsFailure(Exception failure) {
        this.failure = Objects.requireNonNull(failure);
        this.endTimeInMillis = System.currentTimeMillis();
    }

    public synchronized void markAsComplete() {
        this.endTimeInMillis = System.currentTimeMillis();
    }

    synchronized boolean isDone() {
        return this.endTimeInMillis != -1L;
    }
}
