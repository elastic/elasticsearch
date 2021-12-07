/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.structurefinder;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class TextStructureTests extends AbstractSerializingTestCase<TextStructure> {

    @Override
    protected TextStructure createTestInstance() {
        return createTestFileStructure();
    }

    public static TextStructure createTestFileStructure() {

        TextStructure.Format format = randomFrom(EnumSet.allOf(TextStructure.Format.class));

        TextStructure.Builder builder = new TextStructure.Builder(format);

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

        if (format == TextStructure.Format.DELIMITED) {
            builder.setColumnNames(Arrays.asList(generateRandomStringArray(10, 10, false, false)));
            builder.setHasHeaderRow(randomBoolean());
            builder.setDelimiter(randomFrom(',', '\t', ';', '|'));
            builder.setQuote(randomFrom('"', '\''));
        }

        if (format == TextStructure.Format.SEMI_STRUCTURED_TEXT) {
            builder.setGrokPattern(randomAlphaOfLength(100));
        }

        if (format == TextStructure.Format.SEMI_STRUCTURED_TEXT || randomBoolean()) {
            builder.setTimestampField(randomAlphaOfLength(10));
            builder.setJodaTimestampFormats(Arrays.asList(generateRandomStringArray(3, 20, false, false)));
            builder.setJavaTimestampFormats(Arrays.asList(generateRandomStringArray(3, 20, false, false)));
            builder.setNeedClientTimezone(randomBoolean());
        }

        Map<String, Object> mappings = new TreeMap<>();
        for (String field : generateRandomStringArray(5, 20, false, false)) {
            mappings.put(field, Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(10)));
        }
        builder.setMappings(mappings);

        if (randomBoolean()) {
            Map<String, Object> ingestPipeline = new LinkedHashMap<>();
            for (String field : generateRandomStringArray(5, 20, false, false)) {
                ingestPipeline.put(field, Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(10)));
            }
            builder.setMappings(ingestPipeline);
        }

        if (randomBoolean()) {
            Map<String, FieldStats> fieldStats = new TreeMap<>();
            for (String field : generateRandomStringArray(5, 20, false, false)) {
                fieldStats.put(field, FieldStatsTests.createTestFieldStats());
            }
            builder.setFieldStats(fieldStats);
        }

        builder.setExplanation(Arrays.asList(generateRandomStringArray(10, 150, false, false)));

        return builder.build();
    }

    @Override
    protected Writeable.Reader<TextStructure> instanceReader() {
        return TextStructure::new;
    }

    @Override
    protected TextStructure doParseInstance(XContentParser parser) {
        return TextStructure.PARSER.apply(parser, null).build();
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(TextStructure.EXPLAIN, "true"));
    }
}
