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
import org.elasticsearch.xpack.ml.configcreator.FilebeatModuleStore.FilebeatModule;
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

public class TextLogFileStructureFinder extends AbstractLogFileStructureFinder implements LogFileStructureFinder {

    private static final String FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"%s:5044\"]\n";
    private static final String COMMON_LOGSTASH_FILTERS_TEMPLATE = "  grok {\n" +
        "    match => { \"message\" => %s%s%s }\n" +
        "  }\n" +
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
        "    hosts => '%s'\n" +
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
        "    hosts => '%s'\n" +
        "    manage_template => false\n" +
        "    index => \"%s\"\n" +
        "    document_type => \"_doc\"\n" +
        "  }\n" +
        "}\n";
    private static final String FILEBEAT_TO_INGEST_PIPELINE_WITHOUT_MODULE_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.elasticsearch:\n" +
        "  hosts: [\"http://%s:9200\"]\n" +
        "  pipeline: \"%s\"\n";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_WITHOUT_MODULE_TEMPLATE = "PUT _ingest/pipeline/%s\n" +
        "{\n" +
        "  \"description\": \"Ingest pipeline for %s files\",\n" +
        "  \"processors\": [\n" +
        "    {\n" +
        "      \"grok\": {\n" +
        "        \"field\": \"message\",\n" +
        "        \"patterns\": [ \"%s\" ]\n" +
        "      },\n" +
        "      \"date\": {\n" +
        "        \"field\": \"%s\",\n" +
        "%s" +
        "        \"formats\": [ %s ]\n" +
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
        "  hosts: [\"http://%s:9200\"]\n" +
        "  pipeline: \"%s\"\n";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_WITH_MODULE_TEMPLATE = "PUT _ingest/pipeline/%s\n" +
        "%s\n";

    private final FilebeatModuleStore filebeatModuleStore;
    private final List<String> sampleMessages;
    private String filebeatToLogstashConfig;
    private String logstashFromFilebeatConfig;
    private String logstashFromFileConfig;
    private String filebeatToIngestPipelineConfig;
    private String ingestPipelineFromFilebeatConfig;

    TextLogFileStructureFinder(Terminal terminal, FilebeatModuleStore filebeatModuleStore, String sampleFileName, String indexName,
                               String typeName, String elasticsearchHost, String logstashHost, String logstashFileTimezone, String sample,
                               String charsetName, Boolean hasByteOrderMarker) throws UserException {
        super(terminal, sampleFileName, indexName, typeName, elasticsearchHost, logstashHost, logstashFileTimezone, charsetName,
            hasByteOrderMarker);
        this.filebeatModuleStore = filebeatModuleStore;

        String[] sampleLines = sample.split("\n");
        Tuple<TimestampMatch, Set<String>> bestTimestamp = mostLikelyTimestamp(sampleLines);
        if (bestTimestamp == null) {
            // Is it appropriate to treat a file that is neither structured nor has
            // a regular pattern of timestamps as a log file?  Probably not...
            throw new UserException(ExitCodes.DATA_ERROR, "Could not find a timestamp in the log sample provided");
        }

        terminal.println(Verbosity.VERBOSE, "Most likely timestamp format is [" + bestTimestamp.v1() + "]");

        sampleMessages = new ArrayList<>();
        StringBuilder preamble = new StringBuilder();
        int linesConsumed = 0;
        StringBuilder message = null;
        int linesInMessage = 0;
        String multiLineRegex = createMultiLineMessageStartRegex(bestTimestamp.v2(), bestTimestamp.v1().simplePattern.pattern());
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
                    message.append('\n').append(sampleLine);
                    ++linesInMessage;
                }
            }
            if (sampleMessages.size() < 2) {
                preamble.append(sampleLine).append('\n');
            }
        }
        // Don't add the last message, as it might be partial and mess up subsequent pattern finding

        LogFileStructure.Builder structureBuilder = new LogFileStructure.Builder(LogFileStructure.Format.SEMI_STRUCTURED_TEXT)
            .setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(preamble.toString())
            .setNumLinesAnalyzed(linesConsumed)
            .setNumMessagesAnalyzed(sampleMessages.size())
            .setMultilineStartPattern(multiLineRegex);

        SortedMap<String, Object> mappings = new TreeMap<>();
        mappings.put("message", Collections.singletonMap(MAPPING_TYPE_SETTING, "text"));
        mappings.put(DEFAULT_TIMESTAMP_FIELD, Collections.singletonMap(MAPPING_TYPE_SETTING, "date"));

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

        structure = structureBuilder
            .setTimestampField(interimTimestampField)
            .setTimestampFormats(bestTimestamp.v1().dateFormats)
            .setNeedClientTimezone(bestTimestamp.v1().hasTimezoneDependentParsing())
            .setGrokPattern(grokPattern)
            .setMappings(mappings)
            .setExplanation(Collections.singletonList("TODO")) // TODO
            .build();
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

    void createConfigs() {

        String grokPattern = structure.getGrokPattern();
        String grokQuote = bestLogstashQuoteFor(grokPattern);
        String interimTimestampField = structure.getTimestampField();
        String dateFormatsStr = structure.getTimestampFormats().stream().collect(Collectors.joining("\", \"", "\"", "\""));

        String filebeatInputOptions = makeFilebeatInputOptions(structure.getMultilineStartPattern(), null);
        filebeatToLogstashConfig = String.format(Locale.ROOT, FILEBEAT_TO_LOGSTASH_TEMPLATE, filebeatInputOptions,
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        String logstashFromFilebeatFilters = String.format(Locale.ROOT, COMMON_LOGSTASH_FILTERS_TEMPLATE, grokQuote, grokPattern, grokQuote,
            interimTimestampField, dateFormatsStr, interimTimestampField,
            makeLogstashTimezoneSetting(structure.needClientTimezone(), true));
        logstashFromFilebeatConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILEBEAT_TEMPLATE, logstashFromFilebeatFilters,
            elasticsearchHost);
        String logstashFromFileFilters = String.format(Locale.ROOT, COMMON_LOGSTASH_FILTERS_TEMPLATE, grokQuote, grokPattern, grokQuote,
            interimTimestampField, dateFormatsStr, interimTimestampField,
            makeLogstashTimezoneSetting(structure.needClientTimezone(), false));
        logstashFromFileConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILE_TEMPLATE,
            makeLogstashFileInput(structure.getMultilineStartPattern()), logstashFromFileFilters, elasticsearchHost, indexName);
        FilebeatModule matchingModule = (filebeatModuleStore != null) ? filebeatModuleStore.findMatchingModule(sampleMessages) : null;
        if (matchingModule == null) {
            filebeatToIngestPipelineConfig = String.format(Locale.ROOT, FILEBEAT_TO_INGEST_PIPELINE_WITHOUT_MODULE_TEMPLATE,
                filebeatInputOptions, makeFilebeatAddLocaleSetting(structure.needClientTimezone()), elasticsearchHost, typeName);
            String jsonEscapedGrokPattern = grokPattern.replaceAll("([\\\\\"])", "\\\\$1");
            ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, INGEST_PIPELINE_FROM_FILEBEAT_WITHOUT_MODULE_TEMPLATE, typeName,
                typeName, jsonEscapedGrokPattern, interimTimestampField,
                makeIngestPipelineTimezoneSetting(structure.needClientTimezone()), dateFormatsStr, interimTimestampField);
        } else {
            String aOrAn = ("aeiou".indexOf(matchingModule.fileType.charAt(0)) >= 0) ? "an" : "a";
            terminal.println("An existing Filebeat module [" + matchingModule.moduleName +
                "] looks appropriate; the sample file appears to be " + aOrAn + " [" + matchingModule.fileType + "] log");
            filebeatToIngestPipelineConfig = String.format(Locale.ROOT, FILEBEAT_TO_INGEST_PIPELINE_WITH_MODULE_TEMPLATE,
                matchingModule.inputDefinition, elasticsearchHost, typeName);
            ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, INGEST_PIPELINE_FROM_FILEBEAT_WITH_MODULE_TEMPLATE, typeName,
                matchingModule.ingestPipeline);
        }
    }

    static Tuple<TimestampMatch, Set<String>> mostLikelyTimestamp(String[] sampleLines) {

        Map<TimestampMatch, Tuple<Double, Set<String>>> timestampMatches = new LinkedHashMap<>();

        int remainingLines = sampleLines.length;
        double differenceBetweenTwoHighestWeights = 0.0;
        for (String sampleLine : sampleLines) {
            TimestampMatch match = TimestampFormatFinder.findFirstMatch(sampleLine);
            if (match != null) {
                TimestampMatch pureMatch = new TimestampMatch(match.candidateIndex, "", match.dateFormats, match.simplePattern,
                    match.grokPatternName, "");
                timestampMatches.compute(pureMatch, (k, v) -> {
                    if (v == null) {
                        return new Tuple<>(weightForMatch(match.preface), new HashSet<>(Collections.singletonList(match.preface)));
                    } else {
                        v.v2().add(match.preface);
                        return new Tuple<>(v.v1() + weightForMatch(match.preface), v.v2());
                    }
                });
                differenceBetweenTwoHighestWeights = findDifferenceBetweenTwoHighestWeights(timestampMatches.values());
            }
            // The highest possible weight is 1, so if the difference between the two highest weights
            // is less than the number of lines remaining then the leader cannot possibly be overtaken
            if (differenceBetweenTwoHighestWeights > --remainingLines) {
                break;
            }
        }

        double highestWeight = 0.0;
        Tuple<TimestampMatch, Set<String>> highestWeightMatch = null;
        for (Map.Entry<TimestampMatch, Tuple<Double, Set<String>>> entry : timestampMatches.entrySet()) {
            double weight = entry.getValue().v1();
            if (weight > highestWeight) {
                highestWeight = weight;
                highestWeightMatch = new Tuple<>(entry.getKey(), entry.getValue().v2());
            }
        }
        return highestWeightMatch;
    }

    /**
     * Used to weight a timestamp match according to how far along the line it is found.
     * Timestamps at the very beginning of the line are given a weight of 1.  The weight
     * progressively decreases the more text there is preceding the timestamp match, but
     * is always greater than 0.
     * @return A weight in the range (0, 1].
     */
    private static double weightForMatch(String preface) {
        return Math.pow(1.0 + preface.length() / 15.0, -1.1);
    }

    private static double findDifferenceBetweenTwoHighestWeights(Collection<Tuple<Double, Set<String>>> timestampMatches) {
        double highestWeight = 0.0;
        double secondHighestWeight = 0.0;
        for (Tuple<Double, Set<String>> timestampMatch : timestampMatches) {
            double weight = timestampMatch.v1();
            if (weight > highestWeight) {
                secondHighestWeight = highestWeight;
                highestWeight = weight;
            } else if (weight > secondHighestWeight) {
                secondHighestWeight = weight;
            }
        }
        return highestWeight - secondHighestWeight;
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
        if (filebeatToLogstashConfig == null) {
            createConfigs();
            createPreambleComment();
        }

        writeMappingsConfigs(directory, structure.getMappings());

        writeConfigFile(directory, "filebeat-to-logstash.yml", filebeatToLogstashConfig);
        writeConfigFile(directory, "logstash-from-filebeat.conf", logstashFromFilebeatConfig);
        writeConfigFile(directory, "logstash-from-file.conf", logstashFromFileConfig);
        writeConfigFile(directory, "filebeat-to-ingest-pipeline.yml", filebeatToIngestPipelineConfig);
        writeRestCallConfigs(directory, "ingest-pipeline-from-filebeat.console", ingestPipelineFromFilebeatConfig);
    }
}
