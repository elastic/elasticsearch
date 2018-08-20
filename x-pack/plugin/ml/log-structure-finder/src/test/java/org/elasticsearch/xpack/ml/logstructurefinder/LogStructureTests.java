/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class LogStructureTests extends AbstractXContentTestCase<LogStructure> {

    protected LogStructure createTestInstance() {

        LogStructure.Format format = randomFrom(EnumSet.allOf(LogStructure.Format.class));

        LogStructure.Builder builder = new LogStructure.Builder(format);

        int numLinesAnalyzed = randomIntBetween(2, 10000);
        builder.setNumLinesAnalyzed(numLinesAnalyzed);
        int numMessagesAnalyzed = randomIntBetween(1, numLinesAnalyzed);
        builder.setNumMessagesAnalyzed(numMessagesAnalyzed);
        builder.setSampleStart(randomAlphaOfLength(1000));

        String charset = randomFrom(Charset.availableCharsets().keySet());
        builder.setCharset(charset);
        if (charset.toUpperCase(Locale.ROOT).startsWith("UTF")) {
            builder.setHasByteOrderMarker(randomBoolean());
        }

        if (numMessagesAnalyzed < numLinesAnalyzed) {
            builder.setMultilineStartPattern(randomAlphaOfLength(100));
        }
        if (randomBoolean()) {
            builder.setExcludeLinesPattern(randomAlphaOfLength(100));
        }

        if (format.isSeparatedValues() || (format.supportsNesting() && randomBoolean())) {
            builder.setInputFields(Arrays.asList(generateRandomStringArray(10, 10, false, false)));
        }
        if (format.isSeparatedValues()) {
            builder.setHasHeaderRow(randomBoolean());
            if (rarely()) {
                builder.setSeparator(format.separator());
            }
        }
        if (format.isSemiStructured()) {
            builder.setGrokPattern(randomAlphaOfLength(100));
        }

        if (format.isSemiStructured() || randomBoolean()) {
            builder.setTimestampField(randomAlphaOfLength(10));
            builder.setTimestampFormats(Arrays.asList(generateRandomStringArray(3, 20, false, false)));
            builder.setNeedClientTimezone(randomBoolean());
        }

        Map<String, Object> mappings = new TreeMap<>();
        for (String field : generateRandomStringArray(5, 20, false, false)) {
            mappings.put(field, Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(10)));
        }
        builder.setMappings(mappings);

        builder.setExplanation(Arrays.asList(generateRandomStringArray(10, 150, false, false)));

        return builder.build();
    }

    protected LogStructure doParseInstance(XContentParser parser) {
        return LogStructure.PARSER.apply(parser, null).build();
    }

    protected boolean supportsUnknownFields() {
        return false;
    }
}
