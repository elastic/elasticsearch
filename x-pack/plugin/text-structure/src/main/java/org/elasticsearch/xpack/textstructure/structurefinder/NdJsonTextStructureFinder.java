/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FieldStats;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;

/**
 * Newline-delimited JSON.
 */
public class NdJsonTextStructureFinder implements TextStructureFinder {

    private final List<String> sampleMessages;
    private final TextStructure structure;

    static NdJsonTextStructureFinder makeNdJsonTextStructureFinder(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws IOException {

        List<Map<String, ?>> sampleRecords = new ArrayList<>();

        List<String> sampleMessages = Arrays.asList(sample.split("\n"));
        for (String sampleMessage : sampleMessages) {
            XContentParser parser = jsonXContent.createParser(XContentParserConfiguration.EMPTY, sampleMessage);
            sampleRecords.add(parser.mapOrdered());
            timeoutChecker.check("NDJSON parsing");
        }

        TextStructure.Builder structureBuilder = new TextStructure.Builder(TextStructure.Format.NDJSON).setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(sampleMessages.stream().limit(2).collect(Collectors.joining("\n", "", "\n")))
            .setNumLinesAnalyzed(sampleMessages.size())
            .setNumMessagesAnalyzed(sampleRecords.size());

        Tuple<String, TimestampFormatFinder> timeField = TextStructureUtils.guessTimestampField(
            explanation,
            sampleRecords,
            overrides,
            timeoutChecker
        );
        if (timeField != null) {
            boolean needClientTimeZone = timeField.v2().hasTimezoneDependentParsing();

            structureBuilder.setTimestampField(timeField.v1())
                .setJodaTimestampFormats(timeField.v2().getJodaTimestampFormats())
                .setJavaTimestampFormats(timeField.v2().getJavaTimestampFormats())
                .setNeedClientTimezone(needClientTimeZone)
                .setEcsCompatibility(overrides.getEcsCompatibility())
                .setIngestPipeline(
                    TextStructureUtils.makeIngestPipelineDefinition(
                        null,
                        Collections.emptyMap(),
                        null,
                        // Note: no convert processors are added based on mappings for NDJSON input
                        // because it's reasonable that _source matches the supplied JSON precisely
                        Collections.emptyMap(),
                        timeField.v1(),
                        timeField.v2().getJavaTimestampFormats(),
                        needClientTimeZone,
                        timeField.v2().needNanosecondPrecision(),
                        overrides.getEcsCompatibility()
                    )
                );
        }

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, sampleRecords, timeoutChecker);

        Map<String, Object> fieldMappings = mappingsAndFieldStats.v1();
        if (timeField != null) {
            fieldMappings.put(TextStructureUtils.DEFAULT_TIMESTAMP_FIELD, timeField.v2().getEsDateMappingTypeWithoutFormat());
        }

        if (mappingsAndFieldStats.v2() != null) {
            structureBuilder.setFieldStats(mappingsAndFieldStats.v2());
        }

        TextStructure structure = structureBuilder.setMappings(
            Collections.singletonMap(TextStructureUtils.MAPPING_PROPERTIES_SETTING, fieldMappings)
        ).setExplanation(explanation).build();

        return new NdJsonTextStructureFinder(sampleMessages, structure);
    }

    private NdJsonTextStructureFinder(List<String> sampleMessages, TextStructure structure) {
        this.sampleMessages = Collections.unmodifiableList(sampleMessages);
        this.structure = structure;
    }

    @Override
    public List<String> getSampleMessages() {
        return sampleMessages;
    }

    @Override
    public TextStructure getStructure() {
        return structure;
    }
}
