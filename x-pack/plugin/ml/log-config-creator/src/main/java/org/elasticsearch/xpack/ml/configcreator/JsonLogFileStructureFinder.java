/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Really ND-JSON.
 */
public class JsonLogFileStructureFinder extends AbstractStructuredLogFileStructureFinder implements LogFileStructureFinder {

    private final List<String> sampleMessages;
    private final LogFileStructure structure;

    JsonLogFileStructureFinder(Terminal terminal, String sample, String charsetName, Boolean hasByteOrderMarker)
        throws IOException, UserException {
        super(terminal);

        List<Map<String, ?>> sampleRecords = new ArrayList<>();

        sampleMessages = Arrays.asList(sample.split("\n"));
        for (String sampleMessage : sampleMessages) {
            XContentParser parser = jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                sampleMessage);
            sampleRecords.add(parser.map());
        }

        LogFileStructure.Builder structureBuilder = new LogFileStructure.Builder(LogFileStructure.Format.JSON)
            .setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(sampleMessages.stream().limit(2).collect(Collectors.joining("\n", "", "\n")))
            .setNumLinesAnalyzed(sampleMessages.size())
            .setNumMessagesAnalyzed(sampleRecords.size());

        Tuple<String, TimestampMatch> timeField = guessTimestampField(sampleRecords);
        if (timeField != null) {
            structureBuilder.setTimestampField(timeField.v1())
                .setTimestampFormats(timeField.v2().dateFormats)
                .setNeedClientTimezone(timeField.v2().hasTimezoneDependentParsing());
        }

        SortedMap<String, Object> mappings = guessMappings(sampleRecords);
        mappings.put(DEFAULT_TIMESTAMP_FIELD, Collections.singletonMap(MAPPING_TYPE_SETTING, "date"));

        structure = structureBuilder
            .setMappings(mappings)
            .setExplanation(Collections.singletonList("TODO")) // TODO
            .build();
    }

    @Override
    public List<String> getSampleMessages() {
        return Collections.unmodifiableList(sampleMessages);
    }

    @Override
    public LogFileStructure getStructure() {
        return structure;
    }
}
