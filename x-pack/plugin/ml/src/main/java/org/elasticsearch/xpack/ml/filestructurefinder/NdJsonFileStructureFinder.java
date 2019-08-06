/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FieldStats;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;

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
 * Newline-delimited JSON.
 */
public class NdJsonFileStructureFinder implements FileStructureFinder {

    private final List<String> sampleMessages;
    private final FileStructure structure;

    static NdJsonFileStructureFinder makeNdJsonFileStructureFinder(List<String> explanation, String sample, String charsetName,
                                                                   Boolean hasByteOrderMarker, FileStructureOverrides overrides,
                                                                   TimeoutChecker timeoutChecker) throws IOException {

        List<Map<String, ?>> sampleRecords = new ArrayList<>();

        List<String> sampleMessages = Arrays.asList(sample.split("\n"));
        for (String sampleMessage : sampleMessages) {
            XContentParser parser = jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                sampleMessage);
            sampleRecords.add(parser.mapOrdered());
            timeoutChecker.check("NDJSON parsing");
        }

        FileStructure.Builder structureBuilder = new FileStructure.Builder(FileStructure.Format.NDJSON)
            .setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(sampleMessages.stream().limit(2).collect(Collectors.joining("\n", "", "\n")))
            .setNumLinesAnalyzed(sampleMessages.size())
            .setNumMessagesAnalyzed(sampleRecords.size());

        Tuple<String, TimestampFormatFinder> timeField =
            FileStructureUtils.guessTimestampField(explanation, sampleRecords, overrides, timeoutChecker);
        if (timeField != null) {
            boolean needClientTimeZone = timeField.v2().hasTimezoneDependentParsing();

            structureBuilder.setTimestampField(timeField.v1())
                .setJodaTimestampFormats(timeField.v2().getJodaTimestampFormats())
                .setJavaTimestampFormats(timeField.v2().getJavaTimestampFormats())
                .setNeedClientTimezone(needClientTimeZone)
                .setIngestPipeline(FileStructureUtils.makeIngestPipelineDefinition(null, Collections.emptyMap(), timeField.v1(),
                    timeField.v2().getJavaTimestampFormats(), needClientTimeZone));
        }

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats =
            FileStructureUtils.guessMappingsAndCalculateFieldStats(explanation, sampleRecords, timeoutChecker);

        SortedMap<String, Object> mappings = mappingsAndFieldStats.v1();
        if (timeField != null) {
            mappings.put(FileStructureUtils.DEFAULT_TIMESTAMP_FIELD, FileStructureUtils.DATE_MAPPING_WITHOUT_FORMAT);
        }

        if (mappingsAndFieldStats.v2() != null) {
            structureBuilder.setFieldStats(mappingsAndFieldStats.v2());
        }

        FileStructure structure = structureBuilder
            .setMappings(mappings)
            .setExplanation(explanation)
            .build();

        return new NdJsonFileStructureFinder(sampleMessages, structure);
    }

    private NdJsonFileStructureFinder(List<String> sampleMessages, FileStructure structure) {
        this.sampleMessages = Collections.unmodifiableList(sampleMessages);
        this.structure = structure;
    }

    @Override
    public List<String> getSampleMessages() {
        return sampleMessages;
    }

    @Override
    public FileStructure getStructure() {
        return structure;
    }
}
