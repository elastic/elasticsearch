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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;

/**
 * Really ND-JSON.
 */
public class JsonLogFileStructureFinder extends AbstractStructuredLogFileStructureFinder implements LogFileStructureFinder {

    private static final String FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"%s:5044\"]\n";
    private static final String LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
        "  beats {\n" +
        "    port => 5044\n" +
        "    host => \"0.0.0.0\"\n" +
        "  }\n" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  json {\n" +
        "    source => \"message\"\n" +
        "    remove_field => [ \"message\" ]\n" +
        "  }\n" +
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
        "  json {\n" +
        "    source => \"message\"\n" +
        "    remove_field => [ \"message\" ]\n" +
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
    private static final String FILEBEAT_TO_INGEST_PIPELINE_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.elasticsearch:\n" +
        "  hosts: [\"http://%s:9200\"]\n" +
        "  pipeline: \"%s\"\n";
    private static final String INGEST_PIPELINE_DATE_PROCESSOR_TEMPLATE = ",\n" +
        "      \"date\": {\n" +
        "        \"field\": \"%s\",\n" +
        "%s" +
        "        \"formats\": [ %s ]\n" +
        "      }";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_TEMPLATE = "PUT _ingest/pipeline/%s\n" +
        "{\n" +
        "  \"description\": \"Ingest pipeline for %s files\",\n" +
        "  \"processors\": [\n" +
        "    {\n" +
        "      \"json\": {\n" +
        "        \"field\": \"message\",\n" +
        "        \"add_to_root\": true\n" +
        "      }%s\n" +
        "    }\n" +
        "  ]\n" +
        "}\n";

    private String filebeatToLogstashConfig;
    private String logstashFromFilebeatConfig;
    private String logstashFromFileConfig;
    private String filebeatToIngestPipelineConfig;
    private String ingestPipelineFromFilebeatConfig;

    JsonLogFileStructureFinder(Terminal terminal, String sampleFileName, String indexName, String typeName, String elasticsearchHost,
                               String logstashHost, String logstashFileTimezone, String sample, String charsetName,
                               Boolean hasByteOrderMarker) throws IOException, UserException {
        super(terminal, sampleFileName, indexName, typeName, elasticsearchHost, logstashHost, logstashFileTimezone, charsetName,
            hasByteOrderMarker);

        List<Map<String, ?>> sampleRecords = new ArrayList<>();

        String[] sampleLines = sample.split("\n");
        for (String sampleLine : sampleLines) {
            XContentParser parser = jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                sampleLine);
            sampleRecords.add(parser.map());
        }

        LogFileStructure.Builder structureBuilder = new LogFileStructure.Builder(LogFileStructure.Format.JSON)
            .setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(Arrays.stream(sampleLines).limit(2).collect(Collectors.joining("\n", "", "\n")))
            .setNumLinesAnalyzed(sampleLines.length)
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

    void createConfigs() {

        String logstashFromFilebeatDateFilter = "";
        String logstashFromFileDateFilter = "";
        String ingestPipelineDateProcessor = "";
        if (structure.getTimestampField() != null) {
            logstashFromFilebeatDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getTimestampFormats(),
                structure.needClientTimezone(), true);
            logstashFromFileDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getTimestampFormats(),
                structure.needClientTimezone(), false);
            String jsonEscapedField = structure.getTimestampField().replaceAll("([\\\\\"])", "\\\\$1").replace("\t", "\\t");
            ingestPipelineDateProcessor = String.format(Locale.ROOT, INGEST_PIPELINE_DATE_PROCESSOR_TEMPLATE, jsonEscapedField,
                makeIngestPipelineTimezoneSetting(structure.needClientTimezone()),
                structure.getTimestampFormats().stream().collect(Collectors.joining("\", \"", "\"", "\"")));
        }

        String filebeatInputOptions = makeFilebeatInputOptions(null, null);
        filebeatToLogstashConfig = String.format(Locale.ROOT, FILEBEAT_TO_LOGSTASH_TEMPLATE, filebeatInputOptions,
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        logstashFromFilebeatConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILEBEAT_TEMPLATE, logstashFromFilebeatDateFilter,
            elasticsearchHost);
        logstashFromFileConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILE_TEMPLATE, makeLogstashFileInput(null),
            logstashFromFileDateFilter, elasticsearchHost, indexName);
        filebeatToIngestPipelineConfig = String.format(Locale.ROOT, FILEBEAT_TO_INGEST_PIPELINE_TEMPLATE, filebeatInputOptions,
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), elasticsearchHost, typeName);
        ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, INGEST_PIPELINE_FROM_FILEBEAT_TEMPLATE, typeName, typeName,
            ingestPipelineDateProcessor);
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
