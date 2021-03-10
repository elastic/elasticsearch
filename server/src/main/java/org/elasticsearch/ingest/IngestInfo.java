/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.ReportingService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class IngestInfo implements ReportingService.Info {

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
