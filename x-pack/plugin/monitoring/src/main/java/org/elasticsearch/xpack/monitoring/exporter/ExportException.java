/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExportException extends ElasticsearchException implements Iterable<ExportException> {

    private final List<ExportException> exceptions = new ArrayList<>();

    public ExportException(Throwable throwable) {
        super(throwable);
    }

    public ExportException(String msg, Object... args) {
        super(msg, args);
    }

    public ExportException(String msg, Throwable throwable, Object... args) {
        super(msg, throwable, args);
    }

    public ExportException(StreamInput in) throws IOException {
        super(in);
        for (int i = in.readVInt(); i > 0; i--) {
            exceptions.add(new ExportException(in));
        }
    }

    public boolean addExportException(ExportException e) {
        return exceptions.add(e);
    }

    public boolean hasExportExceptions() {
        return exceptions.size() > 0;
    }

    @Override
    public Iterator<ExportException> iterator() {
        return exceptions.iterator();
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeCollection(exceptions);
    }

    @Override
    protected void metadataToXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasExportExceptions()) {
            builder.startArray("exceptions");
            for (ExportException exception : exceptions) {
                builder.startObject();
                exception.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
    }
}
