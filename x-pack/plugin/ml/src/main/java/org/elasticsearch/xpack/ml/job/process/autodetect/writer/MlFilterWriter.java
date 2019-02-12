/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.EQUALS;
import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.NEW_LINE;

public class MlFilterWriter {

    private static final String FILTER_PREFIX = "filter.";

    private final Collection<MlFilter> filters;
    private final StringBuilder buffer;

    public MlFilterWriter(Collection<MlFilter> filters, StringBuilder buffer) {
        this.filters = Objects.requireNonNull(filters);
        this.buffer = Objects.requireNonNull(buffer);
    }

    public void write() throws IOException {
        for (MlFilter filter : filters) {

            StringBuilder filterAsJson = new StringBuilder();
            filterAsJson.append('[');
            boolean first = true;
            for (String item : filter.getItems()) {
                if (first) {
                    first = false;
                } else {
                    filterAsJson.append(',');
                }
                filterAsJson.append('"');
                filterAsJson.append(item);
                filterAsJson.append('"');
            }
            filterAsJson.append(']');
            buffer.append(FILTER_PREFIX).append(filter.getId()).append(EQUALS).append(filterAsJson).append(NEW_LINE);
        }
    }
}
