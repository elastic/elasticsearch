/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.Location;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class XmlTextStructureFinderFactory implements TextStructureFinderFactory {

    private final XMLInputFactory xmlFactory;

    public XmlTextStructureFinderFactory() {
        xmlFactory = XMLInputFactory.newInstance();
        xmlFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE);
        xmlFactory.setProperty(XMLInputFactory.IS_VALIDATING, Boolean.FALSE);
        xmlFactory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        xmlFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        xmlFactory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, Boolean.FALSE);
    }

    @Override
    public boolean canFindFormat(TextStructure.Format format) {
        return format == null || format == TextStructure.Format.XML;
    }

    /**
     * This format matches if the sample consists of one or more XML documents,
     * all with the same root element name.  If there is more than one document,
     * only whitespace is allowed in between them.  The last one does not
     * necessarily have to be complete (as the sample could have truncated it).
     */
    @Override
    public boolean canCreateFromSample(List<String> explanation, String sample, double allowedFractionOfBadLines) {
        int completeDocCount = parseXml(explanation, sample);
        if (completeDocCount == -1) {
            return false;
        }
        if (completeDocCount == 0) {
            explanation.add("Not XML because sample didn't contain a complete document");
            return false;
        }
        explanation.add("Deciding sample is XML");
        return true;
    }

    public boolean canCreateFromMessages(List<String> explanation, List<String> messages, double allowedFractionOfBadLines) {
        for (String message : messages) {
            int completeDocCount = parseXml(explanation, message);
            if (completeDocCount == -1) {
                return false;
            }
            if (completeDocCount == 0) {
                explanation.add("Not XML because a message didn't contain a complete document");
                return false;
            }
            if (completeDocCount > 1) {
                explanation.add("Not XML because a message contains a multiple documents");
                return false;
            }
        }
        explanation.add("Deciding sample is XML");
        return true;
    }

    /**
     * Tries to parse the sample as XML.
     * @return -1 if invalid, otherwise the number of complete docs
     */
    private int parseXml(List<String> explanation, String sample) {
        int completeDocCount = 0;
        String commonRootElementName = null;
        String remainder = sample.trim();
        boolean mightBeAnotherDocument = remainder.isEmpty() == false;

        // This processing is extremely complicated because it's necessary
        // to create a new XML stream reader per document, but each one
        // will read ahead so will potentially consume characters from the
        // following document. We must therefore also recreate the string
        // reader for each document.
        while (mightBeAnotherDocument) {

            try (Reader reader = new StringReader(remainder)) {

                XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(reader);
                try {
                    int nestingLevel = 0;
                    while ((mightBeAnotherDocument = xmlReader.hasNext())) {
                        switch (xmlReader.next()) {
                            case XMLStreamReader.START_ELEMENT:
                                if (nestingLevel++ == 0) {
                                    String rootElementName = xmlReader.getLocalName();
                                    if (commonRootElementName == null) {
                                        commonRootElementName = rootElementName;
                                    } else if (commonRootElementName.equals(rootElementName) == false) {
                                        explanation.add(
                                            "Not XML because different documents have different root "
                                                + "element names: ["
                                                + commonRootElementName
                                                + "] and ["
                                                + rootElementName
                                                + "]"
                                        );
                                        return -1;
                                    }
                                }
                                break;
                            case XMLStreamReader.END_ELEMENT:
                                if (--nestingLevel < 0) {
                                    explanation.add("Not XML because an end element occurs before a start element");
                                    return -1;
                                }
                                break;
                        }
                        if (nestingLevel == 0) {
                            ++completeDocCount;
                            // Find the position that's one character beyond end of the end element.
                            // The next document (if there is one) must start after this (possibly
                            // preceeded by whitespace).
                            Location location = xmlReader.getLocation();
                            int endPos = 0;
                            // Line and column numbers start at 1, not 0
                            for (int wholeLines = location.getLineNumber() - 1; wholeLines > 0; --wholeLines) {
                                endPos = remainder.indexOf('\n', endPos) + 1;
                                if (endPos == 0) {
                                    explanation.add(
                                        "Not XML because XML parser location is inconsistent: line ["
                                            + location.getLineNumber()
                                            + "], column ["
                                            + location.getColumnNumber()
                                            + "] in ["
                                            + remainder
                                            + "]"
                                    );
                                    return -1;
                                }
                            }
                            endPos += location.getColumnNumber() - 1;
                            remainder = remainder.substring(endPos).trim();
                            mightBeAnotherDocument = remainder.isEmpty() == false;
                            break;
                        }
                    }
                } finally {
                    xmlReader.close();
                }
            } catch (IOException | XMLStreamException e) {
                explanation.add("Not XML because there was a parsing exception: [" + e.getMessage().replaceAll("\\s?\r?\n\\s?", " ") + "]");
                return -1;
            }
        }

        return completeDocCount;
    }

    @Override
    public TextStructureFinder createFromSample(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws IOException, ParserConfigurationException, SAXException {
        return XmlTextStructureFinder.makeXmlTextStructureFinder(
            explanation,
            sample,
            charsetName,
            hasByteOrderMarker,
            overrides,
            timeoutChecker
        );
    }

    public TextStructureFinder createFromMessages(
        List<String> explanation,
        List<String> messages,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) throws IOException, ParserConfigurationException, SAXException {
        // XmlTextStructureFinderFactory::canCreateFromMessages already
        // checked that every message contains a single valid XML document,
        // so we can safely concatenate and run the logic for a sample.
        String sample = String.join("\n", messages);
        return XmlTextStructureFinder.makeXmlTextStructureFinder(explanation, sample, "UTF-8", null, overrides, timeoutChecker);
    }
}
