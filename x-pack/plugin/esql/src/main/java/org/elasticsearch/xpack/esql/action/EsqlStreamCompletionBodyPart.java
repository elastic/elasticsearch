/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Final {@link ChunkedRestResponseBodyPart} that writes the completion marker as
 * the last NDJSON line in a streaming ES|QL response. When used as the first and
 * only body part (empty result set), it also includes the columns header.
 */
public class EsqlStreamCompletionBodyPart implements ChunkedRestResponseBodyPart {

    private static final Logger logger = LogManager.getLogger(EsqlStreamCompletionBodyPart.class);

    private final int totalRows;
    private final long tookMillis;
    private final boolean isPartial;
    private final List<ColumnInfoImpl> columns;
    private boolean done = false;

    public EsqlStreamCompletionBodyPart(int totalRows, long tookMillis, boolean isPartial, List<ColumnInfoImpl> columns) {
        this.totalRows = totalRows;
        this.tookMillis = tookMillis;
        this.isPartial = isPartial;
        this.columns = columns;
    }

    @Override
    public boolean isPartComplete() {
        return done;
    }

    @Override
    public boolean isLastPart() {
        return true;
    }

    @Override
    public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
        assert false : "no continuations after completion";
        listener.onFailure(new IllegalStateException("no continuations available"));
    }

    @Override
    public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
        RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(recycler);
        try {
            StringBuilder sb = new StringBuilder();
            if (columns != null) {
                sb.append("{\"columns\":[");
                for (int i = 0; i < columns.size(); i++) {
                    if (i > 0) sb.append(",");
                    sb.append("{\"name\":\"").append(columns.get(i).name()).append("\",");
                    sb.append("\"type\":\"").append(columns.get(i).outputType()).append("\"}");
                }
                sb.append("]}\n");
            }
            String status = isPartial ? "partial" : "ok";
            sb.append("{\"completion\":{\"status\":\"")
                .append(status)
                .append("\",\"total_rows\":")
                .append(totalRows)
                .append(",\"took\":")
                .append(tookMillis)
                .append(",\"is_partial\":")
                .append(isPartial)
                .append("}}\n");
            output.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            done = true;
            return output.moveToBytesReference();
        } catch (Exception e) {
            logger.error("failure encoding completion chunk", e);
            Releasables.closeExpectNoException(output);
            throw e;
        }
    }

    @Override
    public String getResponseContentTypeString() {
        return "application/x-ndjson";
    }
}
