/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer.output;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.process.normalizer.NormalizerResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads normalizer output.
 */
public class NormalizerResultHandler {

    private static final int READ_BUF_SIZE = 1024;

    private final InputStream inputStream;
    private final List<NormalizerResult> normalizedResults;

    public NormalizerResultHandler(InputStream inputStream) {
        this.inputStream = inputStream;
        normalizedResults = new ArrayList<>();
    }

    public List<NormalizerResult> getNormalizedResults() {
        return normalizedResults;
    }

    public void process() throws IOException {
        XContent xContent = XContentFactory.xContent(XContentType.JSON);
        BytesReference bytesRef = null;
        byte[] readBuf = new byte[READ_BUF_SIZE];
        for (int bytesRead = inputStream.read(readBuf); bytesRead != -1; bytesRead = inputStream.read(readBuf)) {
            if (bytesRef == null) {
                bytesRef = new BytesArray(readBuf, 0, bytesRead);
            } else {
                bytesRef = CompositeBytesReference.of(bytesRef, new BytesArray(readBuf, 0, bytesRead));
            }
            bytesRef = parseResults(xContent, bytesRef);
            readBuf = new byte[READ_BUF_SIZE];
        }
    }

    private BytesReference parseResults(XContent xContent, BytesReference bytesRef) throws IOException {
        byte marker = xContent.streamSeparator();
        int from = 0;
        while (true) {
            int nextMarker = findNextMarker(marker, bytesRef, from);
            if (nextMarker == -1) {
                // No more markers in this block
                break;
            }
            // Ignore blank lines
            if (nextMarker > from) {
                parseResult(xContent, bytesRef.slice(from, nextMarker - from));
            }
            from = nextMarker + 1;
        }
        if (from >= bytesRef.length()) {
            return null;
        }
        return bytesRef.slice(from, bytesRef.length() - from);
    }

    private void parseResult(XContent xContent, BytesReference bytesRef) throws IOException {
        try (
            InputStream stream = bytesRef.streamInput();
            XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            NormalizerResult result = NormalizerResult.PARSER.apply(parser, null);
            normalizedResults.add(result);
        }
    }

    private static int findNextMarker(byte marker, BytesReference bytesRef, int from) {
        for (int i = from; i < bytesRef.length(); ++i) {
            if (bytesRef.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }
}
