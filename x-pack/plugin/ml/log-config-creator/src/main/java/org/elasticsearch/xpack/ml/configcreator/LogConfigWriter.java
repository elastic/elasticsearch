/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AccessControlException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.stream.Collectors;

public final class LogConfigWriter {

    private static final boolean IS_WINDOWS = System.getProperty("os.name").startsWith("Windows");

    private static final String BEAT_TIMEZONE_FIELD = "beat.timezone";

    private static final String FILEBEAT_PATH_TEMPLATE = "  paths:\n" +
        "   - '%s'\n";
    private static final String FILEBEAT_ENCODING_TEMPLATE = "  encoding: '%s'\n";
    private static final String FILEBEAT_MULTILINE_CONFIG_TEMPLATE = "  multiline.pattern: '%s'\n" +
        "  multiline.negate: true\n" +
        "  multiline.match: after\n";
    private static final String FILEBEAT_EXCLUDE_LINES_TEMPLATE = "  exclude_lines: ['%s']\n";
    private static final String FILEBEAT_ADD_LOCALE_PROCESSOR = "\n" +
        "processors:\n" +
        "- add_locale: ~\n";

    private static final String LOGSTASH_ENCODING_TEMPLATE = "      charset => \"%s\"\n";
    private static final String LOGSTASH_MULTILINE_CODEC_TEMPLATE = "    codec => multiline {\n" +
        "%s" +
        "      pattern => %s%s%s\n" +
        "      negate => \"true\"\n" +
        "      what => \"previous\"\n" +
        "      auto_flush_interval => 1\n" +
        "    }\n";
    private static final String LOGSTASH_LINE_CODEC_TEMPLATE = "    codec => line {\n" +
        "%s" +
        "    }\n";
    private static final String LOGSTASH_FILE_INPUT_TEMPLATE = "  file {\n" +
        "    type => \"%s\"\n" +
        "    path => [ '%s' ]\n" +
        "    start_position => beginning\n" +
        "    ignore_older => 0\n" +
        "%s" +
        "    sincedb_path => \"" + (IS_WINDOWS ? "NUL" : "/dev/null") + "\"\n" +
        "  }\n";

    private static final String LOGSTASH_TIMEZONE_SETTING_TEMPLATE = "    timezone => \"%s\"\n";

    private static final String INGEST_PIPELINE_DATE_PARSE_TIMEZONE = "        \"timezone\": \"{{ " + BEAT_TIMEZONE_FIELD + " }}\",\n";

    private static final String INDEX_MAPPINGS_TEMPLATE = "PUT %s\n" +
        "%s\n";

    private static final String LOGSTASH_DATE_FILTER_TEMPLATE = "  date {\n" +
        "    match => [ %s%s%s, %s ]\n" +
        "%s" +
        "  }\n";

    private static final String JSON_FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"%s:5044\"]\n";
    private static final String JSON_LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
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
    private static final String JSON_LOGSTASH_FROM_FILE_TEMPLATE = "input {\n" +
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
    private static final String JSON_FILEBEAT_TO_INGEST_PIPELINE_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.elasticsearch:\n" +
        "  hosts: [\"http://%s:9200\"]\n" +
        "  pipeline: \"%s\"\n";
    private static final String JSON_INGEST_PIPELINE_DATE_PROCESSOR_TEMPLATE = ",\n" +
        "      \"date\": {\n" +
        "        \"field\": \"%s\",\n" +
        "%s" +
        "        \"formats\": [ %s ]\n" +
        "      }";
    private static final String JSON_INGEST_PIPELINE_FROM_FILEBEAT_TEMPLATE = "PUT _ingest/pipeline/%s\n" +
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

    private static final String XML_FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"%s:5044\"]\n";
    private static final String XML_LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
        "  beats {\n" +
        "    port => 5044\n" +
        "    host => \"0.0.0.0\"\n" +
        "  }\n" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  xml {\n" +
        "    source => \"message\"\n" +
        "    remove_field => [ \"message\" ]\n" +
        "    target => \"%s\"\n" +
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
    private static final String XML_LOGSTASH_FROM_FILE_TEMPLATE = "input {\n" +
        "%s" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  mutate {\n" +
        "    rename => {\n" +
        "      \"path\" => \"source\"\n" +
        "    }\n" +
        "  }\n" +
        "  xml {\n" +
        "    source => \"message\"\n" +
        "    remove_field => [ \"message\" ]\n" +
        "    target => \"%s\"\n" +
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

    private static final String DELIMITED_FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.logstash:\n" +
        "  hosts: [\"%s:5044\"]\n";
    private static final String SEPARATOR_TEMPLATE = "    separator => %s%c%s\n";
    private static final String QUOTE_CHAR_TEMPLATE = "    quote_char => %s%c%s\n";
    private static final String LOGSTASH_CONVERSIONS_TEMPLATE = "    convert => {\n" +
        "%s" +
        "    }\n";
    private static final String LOGSTASH_STRIP_FILTER_TEMPLATE = "  mutate {\n" +
        "    strip => [ %s ]\n" +
        "  }\n";
    private static final String DELIMITED_LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
        "  beats {\n" +
        "    port => 5044\n" +
        "    host => \"0.0.0.0\"\n" +
        "  }\n" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  csv {\n" +
        "%s" +
        "%s" +
        "    columns => [ %s ]\n" +
        "%s" +
        "    remove_field => [ \"message\" ]\n" +
        "  }\n" +
        "%s" +
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
    private static final String DELIMITED_LOGSTASH_FROM_FILE_TEMPLATE = "input {\n" +
        "%s" +
        "}\n" +
        "\n" +
        "filter {\n" +
        "  mutate {\n" +
        "    rename => {\n" +
        "      \"path\" => \"source\"\n" +
        "    }\n" +
        "  }\n" +
        "  csv {\n" +
        "%s" +
        "%s" +
        "    columns => [ %s ]\n" +
        "%s" +
        "%s" +
        "    remove_field => [ \"message\" ]\n" +
        "  }\n" +
        "%s" +
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

    private static final String TEXT_FILEBEAT_TO_LOGSTASH_TEMPLATE = "filebeat.inputs:\n" +
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
    private static final String TEXT_LOGSTASH_FROM_FILEBEAT_TEMPLATE = "input {\n" +
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
    private static final String TEXT_LOGSTASH_FROM_FILE_TEMPLATE = "input {\n" +
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
    private static final String TEXT_FILEBEAT_TO_INGEST_PIPELINE_WITHOUT_MODULE_TEMPLATE = "filebeat.inputs:\n" +
        "- type: log\n" +
        "%s" +
        "%s" +
        "\n" +
        "output.elasticsearch:\n" +
        "  hosts: [\"http://%s:9200\"]\n" +
        "  pipeline: \"%s\"\n";
    private static final String TEXT_INGEST_PIPELINE_FROM_FILEBEAT_WITHOUT_MODULE_TEMPLATE = "PUT _ingest/pipeline/%s\n" +
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

    private final Terminal terminal;
    private final FilebeatModuleStore filebeatModuleStore;
    private final String sampleFileName;
    private final String indexName;
    private final String typeName;
    private final String elasticsearchHost;
    private final String logstashHost;
    private final String logstashFileTimezone;

    private String preambleComment;
    private String filebeatToLogstashConfig;
    private String logstashFromFilebeatConfig;
    private String logstashFromFileConfig;
    private String filebeatToIngestPipelineConfig;
    private String ingestPipelineFromFilebeatConfig;

    public LogConfigWriter(Terminal terminal, Path filebeatModulePath, String sampleFileName, String indexName, String typeName,
                           String elasticsearchHost, String logstashHost, String logstashFileTimezone) throws IOException {
        this.terminal = Objects.requireNonNull(terminal);
        this.filebeatModuleStore =
            (filebeatModulePath != null) ? new FilebeatModuleStore(terminal, filebeatModulePath, sampleFileName) : null;
        this.sampleFileName = Objects.requireNonNull(sampleFileName);
        this.indexName = Objects.requireNonNull(indexName);
        this.typeName = Objects.requireNonNull(typeName);
        this.elasticsearchHost = Objects.requireNonNull(elasticsearchHost);
        this.logstashHost = Objects.requireNonNull(logstashHost);
        this.logstashFileTimezone = logstashFileTimezone;
    }

    void createPreambleComment(FileStructure structure) {
        String preamble = structure.getSampleStart();
        int lineCount = structure.getNumLinesAnalyzed();
        int messageCount = structure.getNumMessagesAnalyzed();
        if (preamble == null || preamble.isEmpty()) {
            preambleComment = "";
        } else {
            preambleComment = String.format(Locale.ROOT,
                "# This config was derived from a sample of %s messages from %s lines that began with:\n", messageCount, lineCount) +
                "#\n" +
                "# " + preamble.replaceFirst("\n+$", "").replace("\n", "\n# ") + "\n" +
                "#\n";
        }
    }

    private String makeFilebeatAddLocaleSetting(boolean hasTimezoneDependentParsing) {
        return hasTimezoneDependentParsing ? FILEBEAT_ADD_LOCALE_PROCESSOR : "";
    }

    private String makeLogstashTimezoneSetting(boolean hasTimezoneDependentParsing, boolean isFromFilebeat) {
        if (hasTimezoneDependentParsing) {
            String timezone = isFromFilebeat ? "%{" + BEAT_TIMEZONE_FIELD + "}" : logstashFileTimezone;
            if (timezone != null) {
                return String.format(Locale.ROOT, LOGSTASH_TIMEZONE_SETTING_TEMPLATE, timezone);
            }
        }
        return "";
    }

    private String makeLogstashDateFilter(String timeFieldName, List<String> dateFormats, boolean hasTimezoneDependentParsing,
                                            boolean isFromFilebeat) {

        String fieldQuote = bestLogstashQuoteFor(timeFieldName);
        return String.format(Locale.ROOT, LOGSTASH_DATE_FILTER_TEMPLATE, fieldQuote, timeFieldName, fieldQuote,
            dateFormats.stream().collect(Collectors.joining("\", \"", "\"", "\"")),
            makeLogstashTimezoneSetting(hasTimezoneDependentParsing, isFromFilebeat));
    }

    private String makeIngestPipelineTimezoneSetting(boolean hasTimezoneDependentParsing) {
        return hasTimezoneDependentParsing ? INGEST_PIPELINE_DATE_PARSE_TIMEZONE : "";
    }

    private void writeConfigFile(Path directory, String fileName, String contents) throws IOException {
        Path fullPath = directory.resolve(typeName + "-" + fileName);
        Files.write(fullPath, (preambleComment + contents).getBytes(StandardCharsets.UTF_8));
        terminal.println("Wrote config file " + fullPath);
        try {
            Files.setPosixFilePermissions(fullPath, PosixFilePermissions.fromString(fileName.endsWith(".sh") ? "rwxr-xr-x" : "rw-r--r--"));
        } catch (AccessControlException | UnsupportedOperationException e) {
            // * For AccessControlException, assume we're running in an ESTestCase unit test, which will have security manager enabled.
            // * For UnsupportedOperationException, assume we're on Windows.
            // In neither situation is it a problem that the file permissions can't be set.
        }
    }

    private void writeRestCallConfigs(Path directory, String consoleFileName, String consoleCommand) throws IOException {
        writeConfigFile(directory, consoleFileName, consoleCommand);
        String curlCommand = "curl -H 'Content-Type: application/json' -X " +
            consoleCommand.replaceFirst(" ", " http://" + elasticsearchHost + ":9200/").replaceFirst("\n", " -d '\n") + "'\n";
        writeConfigFile(directory, consoleFileName.replaceFirst("\\.console$", ".sh"), curlCommand);
    }

    private void writeMappingsConfigs(Path directory, SortedMap<String, Object> fieldTypes) throws IOException {

        terminal.println(Terminal.Verbosity.VERBOSE, "---");

        Map<String, Object> properties = Collections.singletonMap(FileStructureUtils.MAPPING_PROPERTIES_SETTING, fieldTypes);
        Map<String, Object> docType = Collections.singletonMap("_doc", properties);
        Map<String, Object> mappings = Collections.singletonMap("mappings", docType);

        String fieldTypeMappings = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(mappings);

        writeRestCallConfigs(directory, "index-mappings.console", String.format(Locale.ROOT, INDEX_MAPPINGS_TEMPLATE, indexName,
            fieldTypeMappings));
    }

    static String makeColumnConversions(Map<String, Object> mappings) {
        StringBuilder builder = new StringBuilder();

        for (Map.Entry<String, Object> mapping : mappings.entrySet()) {

            if (mapping.getValue() instanceof Map == false) {
                continue;
            }

            String fieldName = mapping.getKey();
            @SuppressWarnings("unchecked")
            Map<String, String> settings = (Map<String, String>) mapping.getValue();
            if (settings.isEmpty()) {
                continue;
            }

            String convertTo = null;
            switch (settings.get(FileStructureUtils.MAPPING_TYPE_SETTING)){

                case "boolean":
                    convertTo = "boolean";
                    break;
                case "byte":
                case "short":
                case "integer":
                case "long":
                    convertTo = "integer";
                    break;
                case "half_float":
                case "float":
                case "double":
                    convertTo = "float";
                    break;
            }

            if (convertTo != null) {
                String fieldQuote = bestLogstashQuoteFor(fieldName);
                builder.append("      ").append(fieldQuote).append(fieldName).append(fieldQuote)
                    .append(" => \"").append(convertTo).append("\"\n");
            }
        }

        return (builder.length() > 0) ? String.format(Locale.ROOT, LOGSTASH_CONVERSIONS_TEMPLATE, builder.toString()) : "";
    }

    static String bestLogstashQuoteFor(String str) {
        return (str.indexOf('"') >= 0) ? "'" : "\""; // NB: fails if string contains both types of quotes
    }

    private String makeFilebeatInputOptions(String multilineRegex, String excludeLinesRegex, String charsetName) {
        StringBuilder builder = new StringBuilder(String.format(Locale.ROOT, FILEBEAT_PATH_TEMPLATE, sampleFileName));
        if (charsetName.equals(StandardCharsets.UTF_8.name()) == false) {
            builder.append(String.format(Locale.ROOT, FILEBEAT_ENCODING_TEMPLATE, charsetName.toLowerCase(Locale.ROOT)));
        }
        if (multilineRegex != null) {
            builder.append(String.format(Locale.ROOT, FILEBEAT_MULTILINE_CONFIG_TEMPLATE, multilineRegex));
        }
        if (excludeLinesRegex != null) {
            builder.append(String.format(Locale.ROOT, FILEBEAT_EXCLUDE_LINES_TEMPLATE, excludeLinesRegex));

        }
        return builder.toString();
    }

    private String makeLogstashFileInput(String multilineRegex, String charsetName) {
        return String.format(Locale.ROOT, LOGSTASH_FILE_INPUT_TEMPLATE, typeName, sampleFileName,
            makeLogstashFileCodec(multilineRegex, charsetName));
    }

    private String makeLogstashFileCodec(String multilineRegex, String charsetName) {

        String encodingConfig =
            (charsetName.equals(StandardCharsets.UTF_8.name())) ? "" : String.format(Locale.ROOT, LOGSTASH_ENCODING_TEMPLATE, charsetName);
        if (multilineRegex == null) {
            if (encodingConfig.isEmpty()) {
                return "";
            }
            return String.format(Locale.ROOT, LOGSTASH_LINE_CODEC_TEMPLATE, encodingConfig);
        }

        String multilineRegexQuote = bestLogstashQuoteFor(multilineRegex);
        return String.format(Locale.ROOT, LOGSTASH_MULTILINE_CODEC_TEMPLATE, encodingConfig, multilineRegexQuote, multilineRegex,
            multilineRegexQuote);
    }

    private void createJsonConfigs(FileStructure structure) {

        String logstashFromFilebeatDateFilter = "";
        String logstashFromFileDateFilter = "";
        String ingestPipelineDateProcessor = "";
        if (structure.getTimestampField() != null) {
            logstashFromFilebeatDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getJodaTimestampFormats(),
                structure.needClientTimezone(), true);
            logstashFromFileDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getJodaTimestampFormats(),
                structure.needClientTimezone(), false);
            String jsonEscapedField = structure.getTimestampField().replaceAll("([\\\\\"])", "\\\\$1").replace("\t", "\\t");
            ingestPipelineDateProcessor = String.format(Locale.ROOT, JSON_INGEST_PIPELINE_DATE_PROCESSOR_TEMPLATE, jsonEscapedField,
                makeIngestPipelineTimezoneSetting(structure.needClientTimezone()),
                structure.getJodaTimestampFormats().stream().collect(Collectors.joining("\", \"", "\"", "\"")));
        }

        String filebeatInputOptions = makeFilebeatInputOptions(null, null, structure.getCharset());
        filebeatToLogstashConfig = String.format(Locale.ROOT, JSON_FILEBEAT_TO_LOGSTASH_TEMPLATE, filebeatInputOptions,
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        logstashFromFilebeatConfig = String.format(Locale.ROOT, JSON_LOGSTASH_FROM_FILEBEAT_TEMPLATE, logstashFromFilebeatDateFilter,
            elasticsearchHost);
        logstashFromFileConfig = String.format(Locale.ROOT, JSON_LOGSTASH_FROM_FILE_TEMPLATE,
            makeLogstashFileInput(null, structure.getCharset()), logstashFromFileDateFilter, elasticsearchHost, indexName);
        filebeatToIngestPipelineConfig = String.format(Locale.ROOT, JSON_FILEBEAT_TO_INGEST_PIPELINE_TEMPLATE, filebeatInputOptions,
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), elasticsearchHost, typeName);
        ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, JSON_INGEST_PIPELINE_FROM_FILEBEAT_TEMPLATE, typeName, typeName,
            ingestPipelineDateProcessor);
    }

    private void createXmlConfigs(FileStructure structure) {

        assert structure.getMappings().isEmpty() == false;
        String topLevelTag = structure.getMappings().keySet().stream()
            .filter(k -> FileStructureUtils.DEFAULT_TIMESTAMP_FIELD.equals(k) == false).findFirst().get();

        String logstashFromFilebeatDateFilter = "";
        String logstashFromFileDateFilter = "";
        if (structure.getTimestampField() != null) {
            String timeFieldPath = "[" + topLevelTag + "][" + structure.getTimestampField() + "]";
            logstashFromFilebeatDateFilter = makeLogstashDateFilter(timeFieldPath, structure.getJodaTimestampFormats(),
                structure.needClientTimezone(), true);
            logstashFromFileDateFilter = makeLogstashDateFilter(timeFieldPath, structure.getJodaTimestampFormats(),
                structure.needClientTimezone(), false);
        }

        filebeatToLogstashConfig = String.format(Locale.ROOT, XML_FILEBEAT_TO_LOGSTASH_TEMPLATE,
            makeFilebeatInputOptions(structure.getMultilineStartPattern(), null, structure.getCharset()),
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        logstashFromFilebeatConfig = String.format(Locale.ROOT, XML_LOGSTASH_FROM_FILEBEAT_TEMPLATE, topLevelTag,
            logstashFromFilebeatDateFilter, elasticsearchHost);
        logstashFromFileConfig = String.format(Locale.ROOT, XML_LOGSTASH_FROM_FILE_TEMPLATE,
            makeLogstashFileInput(structure.getMultilineStartPattern(), structure.getCharset()), topLevelTag, logstashFromFileDateFilter,
            elasticsearchHost, indexName);
    }

    private void createDelimitedConfigs(FileStructure structure) {

        char delimiter = structure.getDelimiter();
        char quote = structure.getQuote();
        String logstashFromFilebeatDateFilter = "";
        String logstashFromFileDateFilter = "";
        if (structure.getTimestampField() != null) {
            logstashFromFilebeatDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getJodaTimestampFormats(),
                structure.needClientTimezone(), true);
            logstashFromFileDateFilter = makeLogstashDateFilter(structure.getTimestampField(), structure.getJodaTimestampFormats(),
                structure.needClientTimezone(), false);
        }

        filebeatToLogstashConfig = String.format(Locale.ROOT, DELIMITED_FILEBEAT_TO_LOGSTASH_TEMPLATE,
            makeFilebeatInputOptions(structure.getMultilineStartPattern(), structure.getExcludeLinesPattern(), structure.getCharset()),
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        String logstashColumns = structure.getColumnNames().stream()
            .map(column -> (column.indexOf('"') >= 0) ? ("'" + column + "'") : ("\"" + column + "\"")).collect(Collectors.joining(", "));
        String delimiterQuote = bestLogstashQuoteFor(String.valueOf(delimiter));
        String delimiterIfRequired = (delimiter == ',') ? "" : String.format(Locale.ROOT, SEPARATOR_TEMPLATE, delimiterQuote, delimiter,
            delimiterQuote);
        String quoteQuote = bestLogstashQuoteFor(String.valueOf(quote));
        String quoteIfRequired = (quote == '"') ? "" : String.format(Locale.ROOT, QUOTE_CHAR_TEMPLATE, quoteQuote, quote, quoteQuote);
        String logstashColumnConversions = makeColumnConversions(structure.getMappings());
        String logstashStripFilter = Boolean.TRUE.equals(structure.getShouldTrimFields()) ?
            String.format(Locale.ROOT, LOGSTASH_STRIP_FILTER_TEMPLATE, logstashColumns) : "";
        logstashFromFilebeatConfig = String.format(Locale.ROOT, DELIMITED_LOGSTASH_FROM_FILEBEAT_TEMPLATE, delimiterIfRequired,
            quoteIfRequired, logstashColumns, logstashColumnConversions, logstashStripFilter, logstashFromFilebeatDateFilter,
            elasticsearchHost);
        String skipHeaderIfRequired = structure.getHasHeaderRow() ? "    skip_header => true\n": "";
        logstashFromFileConfig = String.format(Locale.ROOT, DELIMITED_LOGSTASH_FROM_FILE_TEMPLATE,
            makeLogstashFileInput(structure.getMultilineStartPattern(), structure.getCharset()), delimiterIfRequired, quoteIfRequired,
            logstashColumns, skipHeaderIfRequired, logstashColumnConversions, logstashStripFilter, logstashFromFileDateFilter,
            elasticsearchHost, indexName);
    }

    private void createTextConfigs(FileStructure structure, List<String> sampleMessages) {

        String grokPattern = structure.getGrokPattern();
        String grokQuote = bestLogstashQuoteFor(grokPattern);
        String interimTimestampField = structure.getTimestampField();
        String dateFormatsStr = structure.getJodaTimestampFormats().stream().collect(Collectors.joining("\", \"", "\"", "\""));

        String filebeatInputOptions = makeFilebeatInputOptions(structure.getMultilineStartPattern(), null, structure.getCharset());
        filebeatToLogstashConfig = String.format(Locale.ROOT, TEXT_FILEBEAT_TO_LOGSTASH_TEMPLATE, filebeatInputOptions,
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        String logstashFromFilebeatFilters = String.format(Locale.ROOT, COMMON_LOGSTASH_FILTERS_TEMPLATE, grokQuote, grokPattern, grokQuote,
            interimTimestampField, dateFormatsStr, interimTimestampField,
            makeLogstashTimezoneSetting(structure.needClientTimezone(), true));
        logstashFromFilebeatConfig = String.format(Locale.ROOT, TEXT_LOGSTASH_FROM_FILEBEAT_TEMPLATE, logstashFromFilebeatFilters,
            elasticsearchHost);
        String logstashFromFileFilters = String.format(Locale.ROOT, COMMON_LOGSTASH_FILTERS_TEMPLATE, grokQuote, grokPattern, grokQuote,
            interimTimestampField, dateFormatsStr, interimTimestampField,
            makeLogstashTimezoneSetting(structure.needClientTimezone(), false));
        logstashFromFileConfig = String.format(Locale.ROOT, TEXT_LOGSTASH_FROM_FILE_TEMPLATE,
            makeLogstashFileInput(structure.getMultilineStartPattern(), structure.getCharset()), logstashFromFileFilters, elasticsearchHost,
            indexName);
        FilebeatModuleStore.FilebeatModule matchingModule =
            (filebeatModuleStore != null) ? filebeatModuleStore.findMatchingModule(sampleMessages) : null;
        if (matchingModule == null) {
            filebeatToIngestPipelineConfig = String.format(Locale.ROOT, TEXT_FILEBEAT_TO_INGEST_PIPELINE_WITHOUT_MODULE_TEMPLATE,
                filebeatInputOptions, makeFilebeatAddLocaleSetting(structure.needClientTimezone()), elasticsearchHost, typeName);
            String jsonEscapedGrokPattern = grokPattern.replaceAll("([\\\\\"])", "\\\\$1");
            ingestPipelineFromFilebeatConfig = String.format(Locale.ROOT, TEXT_INGEST_PIPELINE_FROM_FILEBEAT_WITHOUT_MODULE_TEMPLATE,
                typeName, typeName, jsonEscapedGrokPattern, interimTimestampField,
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

    void createConfigs(FileStructure structure, List<String> sampleMessages) {

        switch (structure.getFormat()) {
            case JSON:
                createJsonConfigs(structure);
                break;
            case XML:
                createXmlConfigs(structure);
                break;
            case DELIMITED:
                createDelimitedConfigs(structure);
                break;
            case SEMI_STRUCTURED_TEXT:
                createTextConfigs(structure, sampleMessages);
                break;
        }
    }

    public synchronized void writeConfigs(FileStructure structure, List<String> sampleMessages, Path directory) throws IOException {

        createConfigs(structure, sampleMessages);
        createPreambleComment(structure);

        writeMappingsConfigs(directory, structure.getMappings());

        writeConfigFile(directory, "filebeat-to-logstash.yml", filebeatToLogstashConfig);
        writeConfigFile(directory, "logstash-from-filebeat.conf", logstashFromFilebeatConfig);
        writeConfigFile(directory, "logstash-from-file.conf", logstashFromFileConfig);

        if (filebeatToIngestPipelineConfig != null) {
            writeConfigFile(directory, "filebeat-to-ingest-pipeline.yml", filebeatToIngestPipelineConfig);
            writeRestCallConfigs(directory, "ingest-pipeline-from-filebeat.console", ingestPipelineFromFilebeatConfig);
        }
    }

    String getPreambleComment() {
        return preambleComment;
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
}
