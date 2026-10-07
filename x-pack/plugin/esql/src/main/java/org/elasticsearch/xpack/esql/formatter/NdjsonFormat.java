/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xcontent.MediaType;

import java.util.Set;

/**
 * Newline-delimited JSON: one JSON document per line, in three kinds of line.
 * <pre>
 * {"columns":[...]}                    header: the column schema
 * {"values":[[...],...]}               one per batch of {@code batch_size} rows
 * {"status":200,"took":N,...}          footer: completion metadata, or the error
 * </pre>
 * The same bytes are produced whether the query result is rendered after the query finishes
 * ({@link NdjsonResponse}) or as rows are computed ({@code EsqlStreamResponseListener}). Every NDJSON byte comes
 * from {@link NdjsonLines}, so a response listener only decides <em>when</em> each line is sent.
 *
 * <p>This is the model for making another format streamable:
 * <ol>
 *   <li>Give the format writers for its three units (header, batch, footer) in this package, as {@link NdjsonLines} does.</li>
 *   <li>Add it to {@code EsqlMediaTypeParser#supportsStreaming} (and {@code hasRecordFraming} if batch boundaries are
 *       visible in its bytes).</li>
 *   <li>Have the streaming listener call those writers.</li>
 * </ol>
 *
 * <p>Unlike the other formats this is <strong>not</strong> registered in {@code EsqlMediaTypeParser.MEDIA_TYPE_REGISTRY},
 * so {@link #headerValues()} is empty. The registry records a format's query parameter only alongside a header value, and
 * both header values that would name NDJSON ({@code application/x-ndjson} and {@code application/vnd.elasticsearch+x-ndjson})
 * already belong to JSON. Registering either would reassign the {@code Accept} header, which must keep returning a single
 * JSON document. {@code EsqlMediaTypeParser} therefore recognises {@code format=ndjson} itself. Do not "fix" this by
 * registering the format.
 */
public final class NdjsonFormat implements MediaType {
    public static final NdjsonFormat INSTANCE = new NdjsonFormat();

    public static final String CONTENT_TYPE = "application/x-ndjson";
    public static final String URL_PARAM_BATCH_SIZE = "batch_size";
    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final int MAX_BATCH_SIZE = 1000;

    private static final String FORMAT = "ndjson";

    private NdjsonFormat() {}

    @Override
    public String queryParameter() {
        return FORMAT;
    }

    @Override
    public Set<HeaderValue> headerValues() {
        return Set.of();
    }

    public static int batchSize(RestRequest request) {
        String param = request.param(URL_PARAM_BATCH_SIZE);
        if (param == null) {
            return DEFAULT_BATCH_SIZE;
        }
        int batchSize;
        try {
            batchSize = Integer.parseInt(param);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + URL_PARAM_BATCH_SIZE + "] must be an integer, got [" + param + "]");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("[" + URL_PARAM_BATCH_SIZE + "] must be at least 1, got [" + batchSize + "]");
        }
        if (batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                "[" + URL_PARAM_BATCH_SIZE + "] must be at most " + MAX_BATCH_SIZE + ", got [" + batchSize + "]"
            );
        }
        return batchSize;
    }
}
