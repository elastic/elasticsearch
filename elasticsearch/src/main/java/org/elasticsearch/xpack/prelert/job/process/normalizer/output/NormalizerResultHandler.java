/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.job.process.normalizer.NormalizerResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads normalizer output.
 */
public class NormalizerResultHandler extends AbstractComponent {

    private static final int READ_BUF_SIZE = 1024;

    private final InputStream inputStream;
    private final List<NormalizerResult> normalizedResults;

    public NormalizerResultHandler(Settings settings, InputStream inputStream) {
        super(settings);
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
                bytesRef = new CompositeBytesReference(bytesRef, new BytesArray(readBuf, 0, bytesRead));
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
        XContentParser parser = xContent.createParser(bytesRef);
        NormalizerResult result = NormalizerResult.PARSER.apply(parser, () -> ParseFieldMatcher.STRICT);
        normalizedResults.add(result);
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

