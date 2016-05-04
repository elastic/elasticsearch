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

package org.elasticsearch.ingest.core;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class IngestInfo implements Writeable, ToXContent {

    private final Set<ProcessorInfo> processors;

    public IngestInfo(List<ProcessorInfo> processors) {
        this.processors = new TreeSet<>(processors);  // we use a treeset here to have a test-able / predictable order
    }

    /**
     * Read from a stream.
     */
    public IngestInfo(StreamInput in) throws IOException {
        processors = new TreeSet<>();
        final int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            processors.add(new ProcessorInfo(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.write(processors.size());
        for (ProcessorInfo info : processors) {
            info.writeTo(out);
        }
    }

    public Iterable<ProcessorInfo> getProcessors() {
        return processors;
    }

    public boolean containsProcessor(String type) {
        return processors.contains(new ProcessorInfo(type));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ingest");
        builder.startArray("processors");
        for (ProcessorInfo info : processors) {
            info.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestInfo that = (IngestInfo) o;
        return Objects.equals(processors, that.processors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(processors);
    }
}
