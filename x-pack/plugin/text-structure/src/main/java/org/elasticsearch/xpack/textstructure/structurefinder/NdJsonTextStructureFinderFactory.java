/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

public class NdJsonTextStructureFinderFactory implements TextStructureFinderFactory {

    @Override
    public boolean canFindFormat(TextStructure.Format format) {
        return format == null || format == TextStructure.Format.NDJSON;
    }

    /**
     * This format matches if the sample consists of one or more NDJSON documents.
     * If there is more than one, they must be newline-delimited.  The
     * documents must be non-empty, to prevent lines containing "{}" from matching.
     */
    @Override
    public boolean canCreateFromSample(List<String> explanation, String sample, double allowedFractionOfBadLines) {

        int completeDocCount = 0;

        try {
            String[] sampleLines = sample.split("\n");
            for (String sampleLine : sampleLines) {
                try (
                    XContentParser parser = jsonXContent.createParser(
                        XContentParserConfiguration.EMPTY,
                        new ContextPrintingStringReader(sampleLine)
                    )
                ) {

                    if (parser.map().isEmpty()) {
                        explanation.add("Not NDJSON because an empty object was parsed: [" + sampleLine + "]");
                        return false;
                    }
                    ++completeDocCount;
                    if (parser.nextToken() != null) {
                        explanation.add(
                            "Not newline delimited NDJSON because a line contained more than a single object: [" + sampleLine + "]"
                        );
                        return false;
                    }
                }
            }
        } catch (IOException | IllegalStateException | XContentParseException e) {
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
    public TextStructureFinder createFromSample(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws IOException {
        return NdJsonTextStructureFinder.makeNdJsonTextStructureFinder(
            explanation,
            sample,
            charsetName,
            hasByteOrderMarker,
            overrides,
            timeoutChecker
        );
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
