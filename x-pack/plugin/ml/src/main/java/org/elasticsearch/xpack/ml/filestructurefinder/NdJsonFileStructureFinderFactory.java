/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;

public class NdJsonFileStructureFinderFactory implements FileStructureFinderFactory {

    @Override
    public boolean canFindFormat(FileStructure.Format format) {
        return format == null || format == FileStructure.Format.NDJSON;
    }

    /**
     * This format matches if the sample consists of one or more NDJSON documents.
     * If there is more than one, they must be newline-delimited.  The
     * documents must be non-empty, to prevent lines containing "{}" from matching.
     */
    @Override
    public boolean canCreateFromSample(List<String> explanation, String sample) {

        int completeDocCount = 0;

        try {
            String[] sampleLines = sample.split("\n");
            for (String sampleLine : sampleLines) {
                try (XContentParser parser = jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new ContextPrintingStringReader(sampleLine))) {

                    if (parser.map().isEmpty()) {
                        explanation.add("Not NDJSON because an empty object was parsed: [" + sampleLine + "]");
                        return false;
                    }
                    ++completeDocCount;
                    if (parser.nextToken() != null) {
                        explanation.add("Not newline delimited NDJSON because a line contained more than a single object: [" +
                            sampleLine + "]");
                        return false;
                    }
                }
            }
        } catch (IOException | IllegalStateException e) {
            explanation.add("Not NDJSON because there was a parsing exception: [" + e.getMessage().replaceAll("\\s?\r?\n\\s?", " ") + "]");
            return false;
        }

        if (completeDocCount == 0) {
            explanation.add("Not NDJSON because sample didn't contain a complete document");
            return false;
        }

        explanation.add("Deciding sample is newline delimited NDJSON");
        return true;
    }

    @Override
    public FileStructureFinder createFromSample(List<String> explanation, String sample, String charsetName, Boolean hasByteOrderMarker,
                                                int lineMergeSizeLimit, FileStructureOverrides overrides, TimeoutChecker timeoutChecker)
        throws IOException {
        return NdJsonFileStructureFinder.makeNdJsonFileStructureFinder(explanation, sample, charsetName, hasByteOrderMarker, overrides,
            timeoutChecker);
    }

    private static class ContextPrintingStringReader extends StringReader {

        private final String str;

        ContextPrintingStringReader(String str) {
            super(str);
            this.str = str;
        }

        @Override
        public String toString() {
            if (str.length() <= 80) {
                return String.format(Locale.ROOT, "\"%s\"", str);
            } else {
                return String.format(Locale.ROOT, "\"%.77s...\"", str);
            }
        }
    }
}
