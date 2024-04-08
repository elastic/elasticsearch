/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FieldStats;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

public class XmlTextStructureFinder implements TextStructureFinder {

    private final List<String> sampleMessages;
    private final TextStructure structure;

    static XmlTextStructureFinder makeXmlTextStructureFinder(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws IOException, ParserConfigurationException, SAXException {

        String messagePrefix;
        try (Scanner scanner = new Scanner(sample)) {
            messagePrefix = scanner.next();
        }

        DocumentBuilderFactory docBuilderFactory = makeDocBuilderFactory();

        List<String> sampleMessages = new ArrayList<>();
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
                sampleMessages.add(sampleDoc);
                linesConsumed += numNewlinesIn(sampleDoc);
                timeoutChecker.check("XML parsing");
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

        // null to allow GC before timestamp search
        sampleDocEnds = null;

        // If we get here the XML parser should have confirmed this
        assert messagePrefix.charAt(0) == '<';
        String topLevelTag = messagePrefix.substring(1);

        TextStructure.Builder structureBuilder = new TextStructure.Builder(TextStructure.Format.XML).setCharset(charsetName)
            .setHasByteOrderMarker(hasByteOrderMarker)
            .setSampleStart(preamble.toString())
            .setNumLinesAnalyzed(linesConsumed)
            .setNumMessagesAnalyzed(sampleRecords.size())
            .setMultilineStartPattern("^\\s*<" + topLevelTag);

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
                        Collections.emptyMap(),
                        topLevelTag + "." + timeField.v1(),
                        timeField.v2().getJavaTimestampFormats(),
                        needClientTimeZone,
                        timeField.v2().needNanosecondPrecision(),
                        overrides.getEcsCompatibility()
                    )
                );
        }

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, sampleRecords, timeoutChecker, overrides.getTimestampFormat());

        if (mappingsAndFieldStats.v2() != null) {
            structureBuilder.setFieldStats(mappingsAndFieldStats.v2());
        }

        Map<String, Object> innerFieldMappings = mappingsAndFieldStats.v1();
        Map<String, Object> secondLevelProperties = new LinkedHashMap<>();
        secondLevelProperties.put(TextStructureUtils.MAPPING_TYPE_SETTING, "object");
        secondLevelProperties.put(TextStructureUtils.MAPPING_PROPERTIES_SETTING, innerFieldMappings);
        SortedMap<String, Object> outerFieldMappings = new TreeMap<>();
        outerFieldMappings.put(topLevelTag, secondLevelProperties);
        if (timeField != null) {
            outerFieldMappings.put(TextStructureUtils.DEFAULT_TIMESTAMP_FIELD, timeField.v2().getEsDateMappingTypeWithoutFormat());
        }

        TextStructure structure = structureBuilder.setMappings(
            Collections.singletonMap(TextStructureUtils.MAPPING_PROPERTIES_SETTING, outerFieldMappings)
        ).setExplanation(explanation).build();

        return new XmlTextStructureFinder(sampleMessages, structure);
    }

    private static DocumentBuilderFactory makeDocBuilderFactory() throws ParserConfigurationException {

        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
        docBuilderFactory.setNamespaceAware(false);
        docBuilderFactory.setValidating(false);
        docBuilderFactory.setXIncludeAware(false);
        docBuilderFactory.setExpandEntityReferences(false);
        docBuilderFactory.setIgnoringComments(true);
        docBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        docBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        // The next 5 should be irrelevant given the previous 1, but it doesn't hurt to set them just in case
        docBuilderFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        docBuilderFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        docBuilderFactory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        docBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        docBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return docBuilderFactory;
    }

    private XmlTextStructureFinder(List<String> sampleMessages, TextStructure structure) {
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

    private static int numNewlinesIn(String str) {
        return (int) str.chars().filter(c -> c == '\n').count();
    }

    private static Map<String, Object> docToMap(Document doc) {

        Map<String, Object> docAsMap = new LinkedHashMap<>();

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
                    Map<String, Object> childNodeAsMap = new LinkedHashMap<>();
                    addNodeToMap(child, childNodeAsMap);
                    if (childNodeAsMap.isEmpty() == false) {
                        nodeAsMap.put(child.getNodeName(), childNodeAsMap);
                    }
                }
            }
        }
    }
}
