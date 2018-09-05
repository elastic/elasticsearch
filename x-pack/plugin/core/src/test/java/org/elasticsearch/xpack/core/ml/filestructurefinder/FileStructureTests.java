/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.filestructurefinder;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class FileStructureTests extends AbstractXContentTestCase<FileStructure> {

    protected FileStructure createTestInstance() {

        FileStructure.Format format = randomFrom(EnumSet.allOf(FileStructure.Format.class));

        FileStructure.Builder builder = new FileStructure.Builder(format);

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

        if (format == FileStructure.Format.DELIMITED || (format.supportsNesting() && randomBoolean())) {
            builder.setInputFields(Arrays.asList(generateRandomStringArray(10, 10, false, false)));
        }
        if (format == FileStructure.Format.DELIMITED) {
            builder.setHasHeaderRow(randomBoolean());
            builder.setDelimiter(randomFrom(',', '\t', ';', '|'));
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

        //if (randomBoolean()) {
            Map<String, FieldStats> fieldStats = new TreeMap<>();
            for (String field : generateRandomStringArray(5, 20, false, false)) {
                fieldStats.put(field, FieldStatsTests.createTestFieldStats());
            }
            builder.setFieldStats(fieldStats);
        //}

        builder.setExplanation(Arrays.asList(generateRandomStringArray(10, 150, false, false)));

        return builder.build();
    }

    protected FileStructure doParseInstance(XContentParser parser) {
        return FileStructure.PARSER.apply(parser, null).build();
    }

    protected boolean supportsUnknownFields() {
        return false;
    }
}
