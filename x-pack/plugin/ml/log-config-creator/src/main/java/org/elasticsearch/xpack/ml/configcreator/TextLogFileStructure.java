/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TextLogFileStructure extends AbstractLogFileStructure implements LogFileStructure {

    private static final String INTERIM_TIMESTAMP_FIELD = "_timestamp";

    private static final String FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"localhost:5044\"]\n";
    private static final String LOGSTASH_FILTERS_TEMPLATE = "filter {\n" +
        "  grok {\n" +
        "    match => { \"message\" => %s%s%s }\n" +
        "  }\n" +
        "%s" +
        "  date {\n" +
        "    match => [ \"" + INTERIM_TIMESTAMP_FIELD + "\", %s ]\n" +
        "    remove_field => [ \"" + INTERIM_TIMESTAMP_FIELD + "\" ]\n" +
        "  }\n" +
        "}\n" ;
    private static final String LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
        "  beats {\n" +
        "    port => 5044\n" +
        "    host => \"0.0.0.0\"\n" +
        "  }\n" +
        "}\n" +
        "\n" +
        "%s" +
        "\n" +
        "output {\n" +
        "  elasticsearch {\n" +
        "    hosts => localhost\n" +
        "    manage_template => false\n" +
        "    index => \"%%{[@metadata][beat]}-%%{[@metadata][version]}-%%{+YYYY.MM.dd}\"\n" +
        "  }\n" +
        "}\n";
    private static final String LOGSTASH_FROM_STDIN_TEMPLATE = "input {\n" +
        "  stdin {%s}\n" +
        "}\n" +
        "\n" +
        "%s" +
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
        "        \"field\": \"" + INTERIM_TIMESTAMP_FIELD + "\",\n" +
        "        \"formats\": [ %s ],\n" +
        "        \"timezone\": \"{{ beat.timezone }}\"\n" +
        "      },\n" +
        "      \"remove\": {\n" +
        "        \"field\": \"" + INTERIM_TIMESTAMP_FIELD + "\"\n" +
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
    private String logstashFromStdinConfig;
    private String filebeatToIngestPipelineConfig;
    private String ingestPipelineFromFilebeatConfig;

    TextLogFileStructure(Terminal terminal, BeatsModuleStore beatsModuleStore, String sampleFileName, String indexName, String typeName,
                         String sample, String charsetName) {
        super(terminal, sampleFileName, indexName, typeName, charsetName);
        this.beatsModuleStore = beatsModuleStore;
        this.sample = sample;
    }

    String getFilebeatToLogstashConfig() {
        return filebeatToLogstashConfig;
    }

    String getLogstashFromFilebeatConfig() {
        return logstashFromFilebeatConfig;
    }

    String getLogstashFromStdinConfig() {
        return logstashFromStdinConfig;
    }

    String getFilebeatToIngestPipelineConfig() {
        return filebeatToIngestPipelineConfig;
    }

    String getIngestPipelineFromFilebeatConfig() {
        return ingestPipelineFromFilebeatConfig;
    }

    void createConfigs() throws Exception {

        String[] sampleLines = sample.split("\n");
        Tuple<TimestampMatch, Set<String>> bestTimestamp = mostCommonTimestamp(sampleLines);
        if (bestTimestamp == null) {
            // Is it appropriate to treat a file that is neither structured nor has
            // a regular pattern of timestamps as a log file?  Probably not...
            throw new Exception("Could not find a timestamp in the log sample provided");
        }

        terminal.println(Verbosity.VERBOSE, "Most common timestamp format is [" + bestTimestamp.v1() + "]");

        List<String> sampleMessages = new ArrayList<>();
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
        }
        // Don't add the last message, as it might be partial and mess up subsequent pattern finding

        mappings = new TreeMap<>();
        mappings.put("message", "text");
        mappings.put(DEFAULT_TIMESTAMP_FIELD, "date");

        // We can't parse directly into @timestamp using Grok, so parse to _timestamp, which the date filter will remove
        String grokPattern = GrokPatternCreator.createGrokPatternFromExamples(terminal, sampleMessages, bestTimestamp.v1().grokPatternName,
            INTERIM_TIMESTAMP_FIELD, mappings);
        String grokQuote = bestLogstashQuoteFor(grokPattern);
        String dateFormatsStr = bestTimestamp.v1().dateFormats.stream().collect(Collectors.joining("\", \"", "\"", "\""));
        String logstashFilters = String.format(Locale.ROOT, LOGSTASH_FILTERS_TEMPLATE, grokQuote, grokPattern, grokQuote,
            makeLogstashFractionalSecondsGsubFilter(INTERIM_TIMESTAMP_FIELD, bestTimestamp.v1()), dateFormatsStr);

        String filebeatInputOptions = makeFilebeatInputOptions(multiLineRegex, null);
        filebeatToLogstashConfig = String.format(Locale.ROOT, FILEBEAT_TO_LOGSTASH_TEMPLATE, filebeatInputOptions);
        logstashFromFilebeatConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILEBEAT_TEMPLATE, logstashFilters);
        logstashFromStdinConfig = String.format(Locale.ROOT, LOGSTASH_FROM_STDIN_TEMPLATE, makeLogstashStdinCodec(multiLineRegex),
            logstashFilters, indexName);
        BeatsModule matchingModule = (beatsModuleStore != null) ? beatsModuleStore.findMatchingModule(sampleMessages) : null;
        if (matchingModule == null) {
            filebeatToIngestPipelineConfig = String.format(Locale.ROOT, FILEBEAT_TO_INGEST_PIPELINE_WITHOUT_MODULE_TEMPLATE,
                filebeatInputOptions, typeName);
            String jsonEscapedGrokPattern = grokPattern.replaceAll("([\\\\\"])", "\\\\$1");
            ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, INGEST_PIPELINE_FROM_FILEBEAT_WITHOUT_MODULE_TEMPLATE, typeName,
                typeName, jsonEscapedGrokPattern,
                makeIngestPipelineFractionalSecondsGsubFilter(INTERIM_TIMESTAMP_FIELD, bestTimestamp.v1()), dateFormatsStr);
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
        writeConfigFile(directory, "logstash-from-stdin.conf", logstashFromStdinConfig);
        writeConfigFile(directory, "filebeat-to-ingest-pipeline.yml", filebeatToIngestPipelineConfig);
        writeRestCallConfigs(directory, "ingest-pipeline-from-filebeat.console", ingestPipelineFromFilebeatConfig);
    }
}
