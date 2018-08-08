/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AccessControlException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

public abstract class AbstractLogFileStructureFinder {

    protected static final boolean IS_WINDOWS = System.getProperty("os.name").startsWith("Windows");
    protected static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";
    protected static final String MAPPING_TYPE_SETTING = "type";
    protected static final String MAPPING_FORMAT_SETTING = "format";
    protected static final String MAPPING_PROPERTIES_SETTING = "properties";

    private static final String BEAT_TIMEZONE_FIELD = "beat.timezone";

    // NUMBER Grok pattern doesn't support scientific notation, so we extend it
    private static final Grok NUMBER_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{NUMBER}(?:[eE][+-]?[0-3]?[0-9]{1,2})?$");
    private static final Grok IP_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{IP}$");
    private static final int KEYWORD_MAX_LEN = 256;
    private static final int KEYWORD_MAX_SPACES = 5;

    private static final String FILEBEAT_PATH_TEMPLATE = "  paths:\n" +
        "   - '%s'\n";
    private static final String FILEBEAT_ENCODING_TEMPLATE = "  encoding: '%s'\n";
    private static final String FILEBEAT_MULTILINE_CONFIG_TEMPLATE = "  multiline.pattern: '%s'\n" +
        "  multiline.negate: true\n" +
        "  multiline.match: after\n";
    private static final String FILEBEAT_EXCLUDE_LINES_TEMPLATE = "  exclude_lines: ['^%s']\n";
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

    protected final Terminal terminal;
    protected final String sampleFileName;
    protected final String indexName;
    protected final String typeName;
    protected final String elasticsearchHost;
    protected final String logstashHost;
    protected final String logstashFileTimezone;
    protected final String charsetName;
    protected final Boolean hasByteOrderMarker;
    protected LogFileStructure structure;
    private String preambleComment = "";

    protected AbstractLogFileStructureFinder(Terminal terminal, String sampleFileName, String indexName, String typeName,
                                             String elasticsearchHost, String logstashHost, String logstashFileTimezone,
                                             String charsetName, Boolean hasByteOrderMarker) {
        this.terminal = Objects.requireNonNull(terminal);
        this.sampleFileName = Objects.requireNonNull(sampleFileName);
        this.indexName = Objects.requireNonNull(indexName);
        this.typeName = Objects.requireNonNull(typeName);
        this.elasticsearchHost = Objects.requireNonNull(elasticsearchHost);
        this.logstashHost = Objects.requireNonNull(logstashHost);
        this.logstashFileTimezone = logstashFileTimezone;
        this.charsetName = Objects.requireNonNull(charsetName);
        this.hasByteOrderMarker = hasByteOrderMarker;
    }

    public LogFileStructure getStructure() {
        return structure;
    }

    protected void writeConfigFile(Path directory, String fileName, String contents) throws IOException {
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

    protected void writeRestCallConfigs(Path directory, String consoleFileName, String consoleCommand) throws IOException {
        writeConfigFile(directory, consoleFileName, consoleCommand);
        String curlCommand = "curl -H 'Content-Type: application/json' -X " +
            consoleCommand.replaceFirst(" ", " http://" + elasticsearchHost + ":9200/").replaceFirst("\n", " -d '\n") + "'\n";
        writeConfigFile(directory, consoleFileName.replaceFirst("\\.console$", ".sh"), curlCommand);
    }

    protected void writeMappingsConfigs(Path directory, SortedMap<String, Object> fieldTypes) throws IOException {

        terminal.println(Verbosity.VERBOSE, "---");

        Map<String, Object> properties = Collections.singletonMap(MAPPING_PROPERTIES_SETTING, fieldTypes);
        Map<String, Object> docType = Collections.singletonMap("_doc", properties);
        Map<String, Object> mappings = Collections.singletonMap("mappings", docType);

        String fieldTypeMappings = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(mappings);

        writeRestCallConfigs(directory, "index-mappings.console", String.format(Locale.ROOT, INDEX_MAPPINGS_TEMPLATE, indexName,
            fieldTypeMappings));
    }

    protected static String bestLogstashQuoteFor(String str) {
        return (str.indexOf('"') >= 0) ? "'" : "\""; // NB: fails if string contains both types of quotes
    }

    protected String makeFilebeatInputOptions(String multilineRegex, String excludeLinesRegex) {
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

    protected String makeLogstashFileInput(String multilineRegex) {
        return String.format(Locale.ROOT, LOGSTASH_FILE_INPUT_TEMPLATE, typeName, sampleFileName, makeLogstashFileCodec(multilineRegex));
    }

    private String makeLogstashFileCodec(String multilineRegex) {

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

    protected static Map<String, String> guessScalarMapping(Terminal terminal, String fieldName, Collection<String> fieldValues) {

        if (fieldValues.stream().allMatch(value -> "true".equals(value) || "false".equals(value))) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "boolean");
        }

        TimestampMatch singleMatch = null;
        for (String fieldValue : fieldValues) {
            if (singleMatch == null) {
                singleMatch = TimestampFormatFinder.findFirstFullMatch(fieldValue);
                if (singleMatch == null) {
                    break;
                }
            } else if (singleMatch.equals(TimestampFormatFinder.findFirstFullMatch(fieldValue, singleMatch.candidateIndex)) == false) {
                singleMatch = null;
                break;
            }
        }
        if (singleMatch != null) {
            return singleMatch.getEsDateMappingTypeWithFormat();
        }

        if (fieldValues.stream().allMatch(NUMBER_GROK::match)) {
            try {
                fieldValues.forEach(Long::parseLong);
                return Collections.singletonMap(MAPPING_TYPE_SETTING, "long");
            } catch (NumberFormatException e) {
                terminal.println(Verbosity.VERBOSE,
                    "Rejecting type 'long' for field [" + fieldName + "] due to parse failure: [" + e.getMessage() + "]");
            }
            try {
                fieldValues.forEach(Double::parseDouble);
                return Collections.singletonMap(MAPPING_TYPE_SETTING, "double");
            } catch (NumberFormatException e) {
                terminal.println(Verbosity.VERBOSE,
                    "Rejecting type 'double' for field [" + fieldName + "] due to parse failure: [" + e.getMessage() + "]");
            }
        }

        else if (fieldValues.stream().allMatch(IP_GROK::match)) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "ip");
        }

        if (fieldValues.stream().anyMatch(AbstractStructuredLogFileStructureFinder::isMoreLikelyTextThanKeyword)) {
            return Collections.singletonMap(MAPPING_TYPE_SETTING, "text");
        }

        return Collections.singletonMap(MAPPING_TYPE_SETTING, "keyword");
    }

    static boolean isMoreLikelyTextThanKeyword(String str) {
        int length = str.length();
        return length > KEYWORD_MAX_LEN || length - str.replaceAll("\\s", "").length() > KEYWORD_MAX_SPACES;
    }

    protected void createPreambleComment() {
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

    protected String makeFilebeatAddLocaleSetting(boolean hasTimezoneDependentParsing) {
        return hasTimezoneDependentParsing ? FILEBEAT_ADD_LOCALE_PROCESSOR : "";
    }

    protected String makeLogstashTimezoneSetting(boolean hasTimezoneDependentParsing, boolean isFromFilebeat) {
        if (hasTimezoneDependentParsing) {
            String timezone = isFromFilebeat ? "%{" + BEAT_TIMEZONE_FIELD + "}" : logstashFileTimezone;
            if (timezone != null) {
                return String.format(Locale.ROOT, LOGSTASH_TIMEZONE_SETTING_TEMPLATE, timezone);
            }
        }
        return "";
    }

    protected String makeIngestPipelineTimezoneSetting(boolean hasTimezoneDependentParsing) {
        return hasTimezoneDependentParsing ? INGEST_PIPELINE_DATE_PARSE_TIMEZONE : "";
    }
}
