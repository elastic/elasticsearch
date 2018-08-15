/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.logstructurefinder;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.logstructurefinder.TimestampFormatFinder.TimestampMatch;

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
public class JsonLogStructureFinder implements LogStructureFinder {

    private final List<String> sampleMessages;
    private final LogStructure structure;

    static JsonLogStructureFinder makeJsonLogStructureFinder(List<String> explanation, String sample, String charsetName,
                                                             Boolean hasByteOrderMarker) throws IOException {

        List<Map<String, ?>> sampleRecords = new ArrayList<>();

        List<String> sampleMessages = Arrays.asList(sample.split("\n"));
        for (String sampleMessage : sampleMessages) {
            XContentParser parser = jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                sampleMessage);
            sampleRecords.add(parser.mapOrdered());
        }

        LogStructure.Builder structureBuilder = new LogStructure.Builder(LogStructure.Format.JSON)
            .setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(sampleMessages.stream().limit(2).collect(Collectors.joining("\n", "", "\n")))
            .setNumLinesAnalyzed(sampleMessages.size())
            .setNumMessagesAnalyzed(sampleRecords.size());

        Tuple<String, TimestampMatch> timeField = LogStructureUtils.guessTimestampField(explanation, sampleRecords);
        if (timeField != null) {
            structureBuilder.setTimestampField(timeField.v1())
                .setTimestampFormats(timeField.v2().dateFormats)
                .setNeedClientTimezone(timeField.v2().hasTimezoneDependentParsing());
        }

        SortedMap<String, Object> mappings = LogStructureUtils.guessMappings(explanation, sampleRecords);
        mappings.put(LogStructureUtils.DEFAULT_TIMESTAMP_FIELD, Collections.singletonMap(LogStructureUtils.MAPPING_TYPE_SETTING, "date"));

        LogStructure structure = structureBuilder
            .setMappings(mappings)
            .setExplanation(explanation)
            .build();

        return new JsonLogStructureFinder(sampleMessages, structure);
    }

    private JsonLogStructureFinder(List<String> sampleMessages, LogStructure structure) {
        this.sampleMessages = Collections.unmodifiableList(sampleMessages);
        this.structure = structure;
    }

    @Override
    public List<String> getSampleMessages() {
        return sampleMessages;
    }

    @Override
    public LogStructure getStructure() {
        return structure;
    }
}
