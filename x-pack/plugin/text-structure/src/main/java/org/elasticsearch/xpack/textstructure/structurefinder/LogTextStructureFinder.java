/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FieldStats;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.joni.exception.SyntaxException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.grok.GrokBuiltinPatterns.ECS_COMPATIBILITY_V1;

public class LogTextStructureFinder implements TextStructureFinder {

    private static final int TOO_MANY_IDENTICAL_DELIMITERS_BEFORE_WILDCARDS = 8;
    private final List<String> sampleMessages;
    private final TextStructure structure;

    private static LogTextStructureFinder makeSingleLineLogTextStructureFinder(
        List<String> explanation,
        String[] sampleLines,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) {
        // treat each line as a single message with no timestamp field

        explanation.add("Timestamp format is explicitly set to \"null\"");

        List<String> sampleMessages = Arrays.asList(sampleLines);

        TextStructure.Builder structureBuilder = new TextStructure.Builder(TextStructure.Format.SEMI_STRUCTURED_TEXT).setCharset(
            charsetName
        )
            .setSampleStart(sampleMessages.stream().limit(2).collect(Collectors.joining("\n", "", "\n")))
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setNumLinesAnalyzed(sampleMessages.size())
            .setNumMessagesAnalyzed(sampleMessages.size());

        Map<String, String> messageMapping = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "text");
        SortedMap<String, Object> fieldMappings = new TreeMap<>();
        fieldMappings.put("message", messageMapping);

        SortedMap<String, FieldStats> fieldStats = new TreeMap<>();
        fieldStats.put("message", TextStructureUtils.calculateFieldStats(messageMapping, sampleMessages, timeoutChecker));

        Map<String, String> customGrokPatternDefinitions = Map.of();

        GrokPatternCreator grokPatternCreator = new GrokPatternCreator(
            explanation,
            sampleMessages,
            fieldMappings,
            fieldStats,
            customGrokPatternDefinitions,
            timeoutChecker,
            ECS_COMPATIBILITY_V1.equals(overrides.getEcsCompatibility())
        );

        String grokPattern = overrides.getGrokPattern();
        if (grokPattern != null) {
            // Since this Grok pattern came from the end user, it might contain a syntax error
            try {
                grokPatternCreator.validateFullLineGrokPattern(grokPattern, "");
            } catch (SyntaxException e) {
                throw new IllegalArgumentException("Supplied Grok pattern [" + grokPattern + "] cannot be converted to a valid regex", e);
            }
        } else {
            grokPattern = grokPatternCreator.createGrokPatternFromExamples(null, null, null);
        }

        TextStructure structure = structureBuilder.setGrokPattern(grokPattern)
            .setEcsCompatibility(overrides.getEcsCompatibility())
            .setIngestPipeline(
                TextStructureUtils.makeIngestPipelineDefinition(
                    grokPattern,
                    customGrokPatternDefinitions,
                    null,
                    fieldMappings,
                    null,
                    null,
                    false,
                    false,
                    overrides.getEcsCompatibility()
                )
            )
            .setMappings(Collections.singletonMap(TextStructureUtils.MAPPING_PROPERTIES_SETTING, fieldMappings))
            .setFieldStats(fieldStats)
            .setExplanation(explanation)
            .build();

        return new LogTextStructureFinder(sampleMessages, structure);
    }

    private static LogTextStructureFinder makeMultiLineLogTextStructureFinder(
        List<String> explanation,
        String[] sampleLines,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) {
        TimestampFormatFinder timestampFormatFinder = populateTimestampFormatFinder(explanation, sampleLines, overrides, timeoutChecker);
        switch (timestampFormatFinder.getNumMatchedFormats()) {
            case 0:
                // To treat text as comprised of multi-line log messages we require the presence
                // of at least one timestamp per message.
                // In cases where it is desired to treat text that is neither structured nor has
                // a regular pattern of timestamps as log messages we require the optional request
                // argument "timestamp_format=null" to be passed, in which case the text will be
                // treated as single line log messages.
                throw new IllegalArgumentException(
                    "Could not find "
                        + ((overrides.getTimestampFormat() == null) ? "a timestamp" : "the specified timestamp format")
                        + " in the sample provided"
                );
            case 1:
                // Simple case
                break;
            default:
                timestampFormatFinder.selectBestMatch();
                break;
        }

        explanation.add(
            ((overrides.getTimestampFormat() == null) ? "Most likely timestamp" : "Timestamp")
                + " format is "
                + timestampFormatFinder.getJavaTimestampFormats()
        );

        List<String> sampleMessages = new ArrayList<>();
        StringBuilder preamble = new StringBuilder();
        int linesConsumed = 0;
        StringBuilder message = null;
        int linesInMessage = 0;
        String multiLineRegex = createMultiLineMessageStartRegex(
            timestampFormatFinder.getPrefaces(),
            timestampFormatFinder.getSimplePattern().pattern()
        );
        Pattern multiLinePattern = Pattern.compile(multiLineRegex);
        for (String sampleLine : sampleLines) {
            if (multiLinePattern.matcher(sampleLine).find()) {
                if (message != null) {
                    sampleMessages.add(message.toString());
                    linesConsumed += linesInMessage;
                }
                message = new StringBuilder(sampleLine);
                linesInMessage = 1;
            } else {
                // If message is null here then the sample probably began with the incomplete ending of a previous message
                if (message == null) {
                    // We count lines before the first message as consumed (just like we would
                    // for the CSV header or lines before the first XML document starts)
                    ++linesConsumed;
                } else {
                    // This check avoids subsequent problems when a massive message is unwieldy and slow to process
                    long lengthAfterAppend = message.length() + 1L + sampleLine.length();
                    if (lengthAfterAppend > lineMergeSizeLimit) {
                        assert linesInMessage > 0;
                        throw new IllegalArgumentException(
                            "Merging lines into messages resulted in an unacceptably long message. "
                                + "Merged message would have ["
                                + (linesInMessage + 1)
                                + "] lines and ["
                                + lengthAfterAppend
                                + "] "
                                + "characters (limit ["
                                + lineMergeSizeLimit
                                + "]). If you have messages this big please increase "
                                + "the value of ["
                                + FindStructureAction.Request.LINE_MERGE_SIZE_LIMIT
                                + "]. Otherwise it "
                                + "probably means the timestamp has been incorrectly detected, so try overriding that."
                        );
                    }
                    message.append('\n').append(sampleLine);
                    ++linesInMessage;
                }
            }
            timeoutChecker.check("multi-line message determination");
            if (sampleMessages.size() < 2) {
                preamble.append(sampleLine).append('\n');
            }
        }
        // Don't add the last message, as it might be partial and mess up subsequent pattern finding

        if (sampleMessages.isEmpty()) {
            throw new IllegalArgumentException(
                "Failed to create more than one message from the sample lines provided. (The "
                    + "last is discarded in case the sample is incomplete.) If your sample does contain multiple messages the "
                    + "problem is probably that the primary timestamp format has been incorrectly detected, so try overriding it."
            );
        }

        // null to allow GC before Grok pattern search
        sampleLines = null;

        TextStructure.Builder structureBuilder = new TextStructure.Builder(TextStructure.Format.SEMI_STRUCTURED_TEXT).setCharset(
            charsetName
        )
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(preamble.toString())
            .setNumLinesAnalyzed(linesConsumed)
            .setNumMessagesAnalyzed(sampleMessages.size())
            .setMultilineStartPattern(multiLineRegex);

        Map<String, String> messageMapping = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "text");
        SortedMap<String, Object> fieldMappings = new TreeMap<>();
        fieldMappings.put("message", messageMapping);
        fieldMappings.put(TextStructureUtils.DEFAULT_TIMESTAMP_FIELD, timestampFormatFinder.getEsDateMappingTypeWithoutFormat());

        SortedMap<String, FieldStats> fieldStats = new TreeMap<>();
        fieldStats.put("message", TextStructureUtils.calculateFieldStats(messageMapping, sampleMessages, timeoutChecker));

        Map<String, String> customGrokPatternDefinitions = timestampFormatFinder.getCustomGrokPatternDefinitions();

        GrokPatternCreator grokPatternCreator = new GrokPatternCreator(
            explanation,
            sampleMessages,
            fieldMappings,
            fieldStats,
            customGrokPatternDefinitions,
            timeoutChecker,
            ECS_COMPATIBILITY_V1.equals(overrides.getEcsCompatibility())
        );

        // We can't parse directly into @timestamp using Grok, so parse to some other time field, which the date filter will then remove
        String interimTimestampField = overrides.getTimestampField();
        String grokPattern = overrides.getGrokPattern();
        if (grokPattern != null) {
            if (interimTimestampField == null) {
                interimTimestampField = "timestamp";
            }
            // Since this Grok pattern came from the end user, it might contain a syntax error
            try {
                grokPatternCreator.validateFullLineGrokPattern(grokPattern, interimTimestampField);
            } catch (SyntaxException e) {
                throw new IllegalArgumentException("Supplied Grok pattern [" + grokPattern + "] cannot be converted to a valid regex", e);
            }
        } else {
            Tuple<String, String> timestampFieldAndFullMatchGrokPattern = grokPatternCreator.findFullLineGrokPattern(interimTimestampField);
            if (timestampFieldAndFullMatchGrokPattern != null) {
                interimTimestampField = timestampFieldAndFullMatchGrokPattern.v1();
                grokPattern = timestampFieldAndFullMatchGrokPattern.v2();
            } else {
                if (interimTimestampField == null) {
                    interimTimestampField = "timestamp";
                }
                grokPattern = grokPatternCreator.createGrokPatternFromExamples(
                    timestampFormatFinder.getGrokPatternName(),
                    timestampFormatFinder.getEsDateMappingTypeWithFormat(),
                    interimTimestampField
                );
            }
        }

        boolean needClientTimeZone = timestampFormatFinder.hasTimezoneDependentParsing();

        TextStructure structure = structureBuilder.setTimestampField(interimTimestampField)
            .setJodaTimestampFormats(timestampFormatFinder.getJodaTimestampFormats())
            .setJavaTimestampFormats(timestampFormatFinder.getJavaTimestampFormats())
            .setNeedClientTimezone(needClientTimeZone)
            .setGrokPattern(grokPattern)
            .setEcsCompatibility(overrides.getEcsCompatibility())
            .setIngestPipeline(
                TextStructureUtils.makeIngestPipelineDefinition(
                    grokPattern,
                    customGrokPatternDefinitions,
                    null,
                    fieldMappings,
                    interimTimestampField,
                    timestampFormatFinder.getJavaTimestampFormats(),
                    needClientTimeZone,
                    timestampFormatFinder.needNanosecondPrecision(),
                    overrides.getEcsCompatibility()
                )
            )
            .setMappings(Collections.singletonMap(TextStructureUtils.MAPPING_PROPERTIES_SETTING, fieldMappings))
            .setFieldStats(fieldStats)
            .setExplanation(explanation)
            .build();

        return new LogTextStructureFinder(sampleMessages, structure);
    }

    static LogTextStructureFinder makeLogTextStructureFinder(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) {
        String[] sampleLines = sample.split("\n");
        if (TextStructureUtils.NULL_TIMESTAMP_FORMAT.equals(overrides.getTimestampFormat())) {
            return makeSingleLineLogTextStructureFinder(
                explanation,
                sampleLines,
                charsetName,
                hasByteOrderMarker,
                lineMergeSizeLimit,
                overrides,
                timeoutChecker
            );
        } else {
            return makeMultiLineLogTextStructureFinder(
                explanation,
                sampleLines,
                charsetName,
                hasByteOrderMarker,
                lineMergeSizeLimit,
                overrides,
                timeoutChecker
            );
        }
    }

    private LogTextStructureFinder(List<String> sampleMessages, TextStructure structure) {
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

    static TimestampFormatFinder populateTimestampFormatFinder(
        List<String> explanation,
        String[] sampleLines,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) {
        TimestampFormatFinder timestampFormatFinder = new TimestampFormatFinder(
            explanation,
            overrides.getTimestampFormat(),
            false,
            false,
            false,
            timeoutChecker,
            ECS_COMPATIBILITY_V1.equals(overrides.getEcsCompatibility())
        );

        for (String sampleLine : sampleLines) {
            timestampFormatFinder.addSample(sampleLine);
        }

        return timestampFormatFinder;
    }

    static String createMultiLineMessageStartRegex(Collection<String> prefaces, String simpleDateRegex) {

        StringBuilder builder = new StringBuilder("^");
        int complexity = GrokPatternCreator.addIntermediateRegex(builder, prefaces);
        builder.append(simpleDateRegex);
        if (builder.substring(0, 3).equals("^\\b")) {
            builder.delete(1, 3);
        }
        // This is here primarily to protect against the horrible patterns that are generated when a not-quite-valid-CSV file
        // has its timestamp column near the end of each line. The algorithm used to produce the multi-line start patterns can
        // then produce patterns like this:
        // ^.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,.*?,\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}
        // If a pattern like this is matched against a line that nearly matches but not quite (which is basically guaranteed in
        // the not-quite-valid-CSV file case) then the backtracking will cause the match attempt to run for many days. Therefore
        // it's better to just error out in this case and let the user try again with overrides.
        if (complexity >= TOO_MANY_IDENTICAL_DELIMITERS_BEFORE_WILDCARDS) {
            throw new IllegalArgumentException(
                "Generated multi-line start pattern based on timestamp position ["
                    + builder
                    + "] is too complex. If your sample is delimited then try overriding the format."
            );
        }
        return builder.toString();
    }
}
