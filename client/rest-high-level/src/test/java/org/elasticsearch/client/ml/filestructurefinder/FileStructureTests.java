/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.filestructurefinder;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

public class FileStructureTests extends AbstractXContentTestCase<FileStructure> {

    @Override
    protected FileStructure createTestInstance() {
        return createTestFileStructure();
    }

    public static FileStructure createTestFileStructure() {

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

        if (format == FileStructure.Format.DELIMITED) {
            builder.setColumnNames(Arrays.asList(generateRandomStringArray(10, 10, false, false)));
            builder.setHasHeaderRow(randomBoolean());
            builder.setDelimiter(randomFrom(',', '\t', ';', '|'));
            builder.setQuote(randomFrom('"', '\''));
        }

        if (format == FileStructure.Format.SEMI_STRUCTURED_TEXT) {
            builder.setGrokPattern(randomAlphaOfLength(100));
        }

        if (format == FileStructure.Format.SEMI_STRUCTURED_TEXT || randomBoolean()) {
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
    protected FileStructure doParseInstance(XContentParser parser) {
        return FileStructure.PARSER.apply(parser, null).build();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // unknown fields are only guaranteed to be ignored at the top level - below this several data
        // structures (e.g. mappings, ingest pipeline, field stats) will preserve arbitrary fields
        return field -> !field.isEmpty();
    }
}
