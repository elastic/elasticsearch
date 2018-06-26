/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ml.configcreator.BeatsModuleStore.BeatsModule;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TextLogFileStructure extends AbstractLogFileStructure implements LogFileStructure {

    private static final String FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "\n" +
        "processors:\n" +
        "- add_locale: ~\n" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"localhost:5044\"]\n";
    private static final String COMMON_LOGSTASH_FILTERS_TEMPLATE =
        "  grok {\n" +
        "    match => { \"message\" => %s%s%s }\n" +
        "  }\n" +
        "%s" +
        "  date {\n" +
        "    match => [ \"%s\", %s ]\n" +
        "    remove_field => [ \"%s\" ]\n" +
        "%s" +
        "  }\n";
    private static final String LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
        "  beats {\n" +
        "    port => 5044\n" +
        "    host => \"0.0.0.0\"\n" +
        "  }\n" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "%s" +
        "}\n" +
        "\n" +
        "output {\n" +
        "  elasticsearch {\n" +
        "    hosts => localhost\n" +
        "    manage_template => false\n" +
        "    index => \"%%{[@metadata][beat]}-%%{[@metadata][version]}-%%{+YYYY.MM.dd}\"\n" +
        "  }\n" +
        "}\n";
    private static final String LOGSTASH_FROM_FILE_TEMPLATE = "input {\n" +
        "%s" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  mutate {\n" +
        "    rename => {\n" +
        "      \"path\" => \"source\"\n" +
        "    }\n" +
        "  }\n" +
        "%s" +
        "}\n" +
        "\n" +
        "output {\n" +
        "  elasticsearch {\n" +
        "    hosts => localhost\n" +
        "    manage_template => false\n" +
        "    index => \"%s\"\n" +
        "    document_type => \"_doc\"\n" +
        "  }\n" +
        "}\n";
    private static final String FILEBEAT_TO_INGEST_PIPELINE_WITHOUT_MODULE_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "\n" +
        "processors:\n" +
        "- add_locale: ~\n" +
        "\n" +
        "output.elasticsearch:\n" +
        "  hosts: [\"http://localhost:9200\"]\n" +
        "  pipeline: \"%s\"\n";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_WITHOUT_MODULE_TEMPLATE = "PUT _ingest/pipeline/%s\n" +
        "{\n" +
        "  \"description\": \"Ingest pipeline for %s files\",\n" +
        "  \"processors\": [\n" +
        "    {\n" +
        "      \"grok\": {\n" +
        "        \"field\": \"message\",\n" +
        "        \"patterns\": [ \"%s\" ]\n" +
        "      }%s,\n" +
        "      \"date\": {\n" +
        "        \"field\": \"%s\",\n" +
        "        \"formats\": [ %s ],\n" +
        "        \"timezone\": \"{{ " + BEAT_TIMEZONE_FIELD + " }}\"\n" +
        "      },\n" +
        "      \"remove\": {\n" +
        "        \"field\": \"%s\"\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}\n";
    private static final String FILEBEAT_TO_INGEST_PIPELINE_WITH_MODULE_TEMPLATE = "filebeat.inputs:\n" +
        "%s\n" +
        "\n" +
        "output.elasticsearch:\n" +
        "  hosts: [\"http://localhost:9200\"]\n" +
        "  pipeline: \"%s\"\n";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_WITH_MODULE_TEMPLATE = "PUT _ingest/pipeline/%s\n" +
        "%s\n";

    private final String sample;
    private final BeatsModuleStore beatsModuleStore;
    private SortedMap<String, String> mappings;
    private String filebeatToLogstashConfig;
    private String logstashFromFilebeatConfig;
    private String logstashFromFileConfig;
    private String filebeatToIngestPipelineConfig;
    private String ingestPipelineFromFilebeatConfig;

    TextLogFileStructure(Terminal terminal, BeatsModuleStore beatsModuleStore, String sampleFileName, String indexName, String typeName,
                         String logstashFileTimezone, String sample, String charsetName) {
        super(terminal, sampleFileName, indexName, typeName, logstashFileTimezone, charsetName);
        this.beatsModuleStore = beatsModuleStore;
        this.sample = Objects.requireNonNull(sample);
    }

    String getFilebeatToLogstashConfig() {
        return filebeatToLogstashConfig;
    }

    String getLogstashFromFilebeatConfig() {
        return logstashFromFilebeatConfig;
    }

    String getLogstashFromFileConfig() {
        return logstashFromFileConfig;
    }

    String getFilebeatToIngestPipelineConfig() {
        return filebeatToIngestPipelineConfig;
    }

    String getIngestPipelineFromFilebeatConfig() {
        return ingestPipelineFromFilebeatConfig;
    }

    void createConfigs() throws UserException {

        String[] sampleLines = sample.split("\n");
        Tuple<TimestampMatch, Set<String>> bestTimestamp = mostCommonTimestamp(sampleLines);
        if (bestTimestamp == null) {
            // Is it appropriate to treat a file that is neither structured nor has
            // a regular pattern of timestamps as a log file?  Probably not...
            throw new UserException(ExitCodes.DATA_ERROR, "Could not find a timestamp in the log sample provided");
        }

        terminal.println(Verbosity.VERBOSE, "Most common timestamp format is [" + bestTimestamp.v1() + "]");

        List<String> sampleMessages = new ArrayList<>();
        StringBuilder preamble = new StringBuilder();
        StringBuilder message = null;
        String multiLineRegex = createMultiLineMessageStartRegex(bestTimestamp.v2(), bestTimestamp.v1().simplePattern.pattern());
        Pattern multiLinePattern = Pattern.compile(multiLineRegex);
        for (String sampleLine : sampleLines) {
            if (multiLinePattern.matcher(sampleLine).find()) {
                if (message != null) {
                    sampleMessages.add(message.toString());
                }
                message = new StringBuilder(sampleLine);
            } else {
                // If message is null here then the sample probably began with the incomplete ending of a previous message
                if (message != null) {
                    message.append('\n').append(sampleLine);
                }
            }
            if (sampleMessages.size() < 2) {
                preamble.append(sampleLine).append('\n');
            }
        }
        // Don't add the last message, as it might be partial and mess up subsequent pattern finding

        createPreambleComment(preamble.toString());

        mappings = new TreeMap<>();
        mappings.put("message", "text");
        mappings.put(DEFAULT_TIMESTAMP_FIELD, "date");

        // We can't parse directly into @timestamp using Grok, so parse to some other time field, which the date filter will then remove
        String interimTimestampField;
        String grokPattern;
        Tuple<String, String> timestampFieldAndFullMatchGrokPattern =
            GrokPatternCreator.findFullLineGrokPattern(terminal, sampleMessages, mappings);
        if (timestampFieldAndFullMatchGrokPattern != null) {
            interimTimestampField = timestampFieldAndFullMatchGrokPattern.v1();
            grokPattern = timestampFieldAndFullMatchGrokPattern.v2();
        } else {
            interimTimestampField = "timestamp";
            grokPattern = GrokPatternCreator.createGrokPatternFromExamples(terminal, sampleMessages, bestTimestamp.v1().grokPatternName,
                interimTimestampField, mappings);
        }
        String grokQuote = bestLogstashQuoteFor(grokPattern);
        String dateFormatsStr = bestTimestamp.v1().dateFormats.stream().collect(Collectors.joining("\", \"", "\"", "\""));

        String filebeatInputOptions = makeFilebeatInputOptions(multiLineRegex, null);
        filebeatToLogstashConfig = String.format(Locale.ROOT, FILEBEAT_TO_LOGSTASH_TEMPLATE, filebeatInputOptions);
        String logstashFromFilebeatFilters = String.format(Locale.ROOT, COMMON_LOGSTASH_FILTERS_TEMPLATE, grokQuote, grokPattern, grokQuote,
            makeLogstashFractionalSecondsGsubFilter(interimTimestampField, bestTimestamp.v1()), interimTimestampField, dateFormatsStr,
            interimTimestampField, makeLogstashTimezoneSetting(true));
        logstashFromFilebeatConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILEBEAT_TEMPLATE, logstashFromFilebeatFilters);
        String logstashFromFileFilters = String.format(Locale.ROOT, COMMON_LOGSTASH_FILTERS_TEMPLATE, grokQuote, grokPattern, grokQuote,
            makeLogstashFractionalSecondsGsubFilter(interimTimestampField, bestTimestamp.v1()), interimTimestampField, dateFormatsStr,
            interimTimestampField, makeLogstashTimezoneSetting(false));
        logstashFromFileConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILE_TEMPLATE, makeLogstashFileInput(multiLineRegex),
            logstashFromFileFilters, indexName);
        BeatsModule matchingModule = (beatsModuleStore != null) ? beatsModuleStore.findMatchingModule(sampleMessages) : null;
        if (matchingModule == null) {
            filebeatToIngestPipelineConfig = String.format(Locale.ROOT, FILEBEAT_TO_INGEST_PIPELINE_WITHOUT_MODULE_TEMPLATE,
                filebeatInputOptions, typeName);
            String jsonEscapedGrokPattern = grokPattern.replaceAll("([\\\\\"])", "\\\\$1");
            ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, INGEST_PIPELINE_FROM_FILEBEAT_WITHOUT_MODULE_TEMPLATE, typeName,
                typeName, jsonEscapedGrokPattern,
                makeIngestPipelineFractionalSecondsGsubFilter(interimTimestampField, bestTimestamp.v1()), interimTimestampField,
                dateFormatsStr, interimTimestampField);
        } else {
            String aOrAn = ("aeiou".indexOf(matchingModule.fileType.charAt(0)) >= 0) ? "an" : "a";
            terminal.println("An existing filebeat module [" + matchingModule.moduleName +
                "] looks appropriate; the sample file appears to be " + aOrAn + " [" + matchingModule.fileType + "] log");
            filebeatToIngestPipelineConfig = String.format(Locale.ROOT, FILEBEAT_TO_INGEST_PIPELINE_WITH_MODULE_TEMPLATE,
                matchingModule.inputDefinition, typeName);
            ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, INGEST_PIPELINE_FROM_FILEBEAT_WITH_MODULE_TEMPLATE, typeName,
                matchingModule.ingestPipeline);
        }
    }

    private static Tuple<TimestampMatch, Set<String>> mostCommonTimestamp(String[] sampleLines) {

        Map<TimestampMatch, Tuple<Integer, Set<String>>> timestampMatches = new LinkedHashMap<>();

        int remainingLines = sampleLines.length;
        int differenceBetweenMostCommonAndSecondMostCommonCounts = 0;
        for (String sampleLine : sampleLines) {
            TimestampMatch match = TimestampFormatFinder.findFirstMatch(sampleLine);
            if (match != null) {
                TimestampMatch pureMatch = new TimestampMatch(match.candidateIndex, "", match.dateFormats, match.simplePattern,
                    match.grokPatternName, "", match.hasFractionalComponentSmallerThanMillisecond);
                timestampMatches.compute(pureMatch, (k, v) -> {
                    if (v == null) {
                        return new Tuple<>(1, new HashSet<>(Collections.singletonList(match.preface)));
                    } else {
                        v.v2().add(match.preface);
                        return new Tuple<>(v.v1() + 1, v.v2());
                    }
                });
                differenceBetweenMostCommonAndSecondMostCommonCounts =
                    findDifferenceBetweenMostCommonAndSecondMostCommonCounts(timestampMatches.values());
            }
            if (differenceBetweenMostCommonAndSecondMostCommonCounts > --remainingLines) {
                break;
            }
        }

        int mostCommonCount = 0;
        Tuple<TimestampMatch, Set<String>> mostCommonMatch = null;
        for (Map.Entry<TimestampMatch, Tuple<Integer, Set<String>>> entry : timestampMatches.entrySet()) {
            int count = entry.getValue().v1();
            if (count > mostCommonCount) {
                mostCommonCount = count;
                mostCommonMatch = new Tuple<>(entry.getKey(), entry.getValue().v2());
            }
        }
        return mostCommonMatch;
    }

    private static int findDifferenceBetweenMostCommonAndSecondMostCommonCounts(Collection<Tuple<Integer, Set<String>>> timestampMatches) {
        int mostCommonCount = 0;
        int secondMostCommonCount = 0;
        for (Tuple<Integer, Set<String>> timestampMatch : timestampMatches) {
            int count = timestampMatch.v1();
            if (count > mostCommonCount) {
                secondMostCommonCount = mostCommonCount;
                mostCommonCount = count;
            } else if (count > secondMostCommonCount) {
                secondMostCommonCount = count;
            }
        }
        return mostCommonCount - secondMostCommonCount;
    }

    static String createMultiLineMessageStartRegex(Collection<String> prefaces, String timestampRegex) {

        StringBuilder builder = new StringBuilder("^");
        GrokPatternCreator.addIntermediateRegex(builder, prefaces);
        builder.append(timestampRegex);
        if (builder.substring(0, 3).equals("^\\b")) {
            builder.delete(1, 3);
        }
        return builder.toString();
    }

    @Override
    public synchronized void writeConfigs(Path directory) throws Exception {
        if (mappings == null) {
            createConfigs();
        }

        writeMappingsConfigs(directory, mappings);

        writeConfigFile(directory, "filebeat-to-logstash.yml", filebeatToLogstashConfig);
        writeConfigFile(directory, "logstash-from-filebeat.conf", logstashFromFilebeatConfig);
        writeConfigFile(directory, "logstash-from-file.conf", logstashFromFileConfig);
        writeConfigFile(directory, "filebeat-to-ingest-pipeline.yml", filebeatToIngestPipelineConfig);
        writeRestCallConfigs(directory, "ingest-pipeline-from-filebeat.console", ingestPipelineFromFilebeatConfig);
    }
}
