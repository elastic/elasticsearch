/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class XmlLogFileStructureFinder extends AbstractStructuredLogFileStructureFinder implements LogFileStructureFinder {

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

    private String filebeatToLogstashConfig;
    private String logstashFromFilebeatConfig;
    private String logstashFromFileConfig;

    XmlLogFileStructureFinder(Terminal terminal, String sampleFileName, String indexName, String typeName, String elasticsearchHost,
                              String logstashHost, String logstashFileTimezone, String sample, String charsetName,
                              Boolean hasByteOrderMarker) throws IOException, ParserConfigurationException, SAXException, UserException {
        super(terminal, sampleFileName, indexName, typeName, elasticsearchHost, logstashHost, logstashFileTimezone, charsetName,
            hasByteOrderMarker);

        String messagePrefix;
        try (Scanner scanner = new Scanner(sample)) {
            messagePrefix = scanner.next();
        }

        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setNamespaceAware(false);
        docBuilderFactory.setValidating(false);

        List<Map<String, ?>> sampleRecords = new ArrayList<>();

        String[] sampleDocEnds = sample.split(Pattern.quote(messagePrefix));
        StringBuilder preamble = new StringBuilder(sampleDocEnds[0]);
        int linesConsumed = numNewlinesIn(sampleDocEnds[0]);
        for (int i = 1; i < sampleDocEnds.length; ++i) {
            String sampleDoc = messagePrefix + sampleDocEnds[i];
            if (i < 3) {
                preamble.append(sampleDoc);
            }
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            try (InputStream is = new ByteArrayInputStream(sampleDoc.getBytes(StandardCharsets.UTF_8))) {
                sampleRecords.add(docToMap(docBuilder.parse(is)));
                linesConsumed += numNewlinesIn(sampleDoc);
            } catch (SAXException e) {
                // Tolerate an incomplete last record as long as we have one complete record
                if (sampleRecords.isEmpty() || i < sampleDocEnds.length - 1) {
                    throw e;
                }
            }
        }

        if (sample.endsWith("\n") == false) {
            ++linesConsumed;
        }

        // If we get here the XML parser should have confirmed this
        assert messagePrefix.charAt(0) == '<';
        String topLevelTag = messagePrefix.substring(1);

        LogFileStructure.Builder structureBuilder = new LogFileStructure.Builder(LogFileStructure.Format.XML)
            .setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(preamble.toString())
            .setNumLinesAnalyzed(linesConsumed)
            .setNumMessagesAnalyzed(sampleRecords.size())
            .setMultilineStartPattern("^\\s*<" + topLevelTag);

        Tuple<String, TimestampMatch> timeField = guessTimestampField(sampleRecords);
        if (timeField != null) {
            structureBuilder.setTimestampField(timeField.v1())
                .setTimestampFormats(timeField.v2().dateFormats)
                .setNeedClientTimezone(timeField.v2().hasTimezoneDependentParsing());
        }

        SortedMap<String, Object> innerMappings = guessMappings(sampleRecords);
        Map<String, Object> secondLevelProperties = new LinkedHashMap<>();
        secondLevelProperties.put(MAPPING_TYPE_SETTING, "object");
        secondLevelProperties.put(MAPPING_PROPERTIES_SETTING, innerMappings);
        SortedMap<String, Object> outerMappings = new TreeMap<>();
        outerMappings.put(topLevelTag, secondLevelProperties);
        outerMappings.put(DEFAULT_TIMESTAMP_FIELD, Collections.singletonMap(MAPPING_TYPE_SETTING, "date"));

        structure = structureBuilder
            .setMappings(outerMappings)
            .setExplanation(Collections.singletonList("TODO")) // TODO
            .build();
    }

    private static int numNewlinesIn(String str) {
        return (int) str.chars().filter(c -> c == '\n').count();
    }

    private static Map<String, Object> docToMap(Document doc) {

        Map<String, Object> docAsMap = new HashMap<>();

        doc.getDocumentElement().normalize();
        addNodeToMap(doc.getDocumentElement(), docAsMap);

        return docAsMap;
    }

    private static void addNodeToMap(Node node, Map<String, Object> nodeAsMap) {

        NamedNodeMap attributes = node.getAttributes();
        for (int i = 0; i < attributes.getLength(); ++i) {
            Node attribute = attributes.item(i);
            nodeAsMap.put(attribute.getNodeName(), attribute.getNodeValue());
        }

        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); ++i) {
            Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE) {
                if (child.getChildNodes().getLength() == 1) {
                    Node grandChild = child.getChildNodes().item(0);
                    String value = grandChild.getNodeValue().trim();
                    if (value.isEmpty() == false) {
                        nodeAsMap.put(child.getNodeName(), value);
                    }
                } else {
                    Map<String, Object> childNodeAsMap = new HashMap<>();
                    addNodeToMap(child, childNodeAsMap);
                    if (childNodeAsMap.isEmpty() == false) {
                        nodeAsMap.put(child.getNodeName(), childNodeAsMap);
                    }
                }
            }
        }
    }

    void createConfigs() {

        assert structure.getMappings().isEmpty() == false;
        String topLevelTag =
            structure.getMappings().keySet().stream().filter(k -> DEFAULT_TIMESTAMP_FIELD.equals(k) == false).findFirst().get();

        String logstashFromFilebeatDateFilter = "";
        String logstashFromFileDateFilter = "";
        if (structure.getTimestampField() != null) {
            String timeFieldPath = "[" + topLevelTag + "][" + structure.getTimestampField() + "]";
            logstashFromFilebeatDateFilter = makeLogstashDateFilter(timeFieldPath, structure.getTimestampFormats(),
                structure.needClientTimezone(), true);
            logstashFromFileDateFilter = makeLogstashDateFilter(timeFieldPath, structure.getTimestampFormats(),
                structure.needClientTimezone(), false);
        }

        filebeatToLogstashConfig = String.format(Locale.ROOT, FILEBEAT_TO_LOGSTASH_TEMPLATE,
            makeFilebeatInputOptions(structure.getMultilineStartPattern(), null),
            makeFilebeatAddLocaleSetting(structure.needClientTimezone()), logstashHost);
        logstashFromFilebeatConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILEBEAT_TEMPLATE, topLevelTag,
            logstashFromFilebeatDateFilter, elasticsearchHost);
        logstashFromFileConfig = String.format(Locale.ROOT, LOGSTASH_FROM_FILE_TEMPLATE,
            makeLogstashFileInput(structure.getMultilineStartPattern()), topLevelTag, logstashFromFileDateFilter, elasticsearchHost,
            indexName);
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
    }
}
