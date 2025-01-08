/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.idea;

import groovy.util.Node;
import groovy.util.XmlParser;
import groovy.xml.XmlNodePrinter;

import org.gradle.api.Action;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import javax.xml.parsers.ParserConfigurationException;

public class IdeaXmlUtil {

    static Node parseXml(String xmlPath) throws IOException, SAXException, ParserConfigurationException {
        File xmlFile = new File(xmlPath);
        XmlParser xmlParser = new XmlParser(false, true, true);
        xmlParser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        Node xml = xmlParser.parse(xmlFile);
        return xml;
    }

    /**
     * Parses a given XML file, applies a set of changes, and writes those changes back to the original file.
     *
     * @param path Path to existing XML file
     * @param action Action to perform on parsed XML document
     * but before the XML document, e.g. a doctype or comment
     */
    static void modifyXml(String xmlPath, Action<? super Node> action) throws IOException, ParserConfigurationException, SAXException {
        modifyXml(xmlPath, action, null);
    }

    /**
     * Parses a given XML file, applies a set of changes, and writes those changes back to the original file.
     *
     * @param path Path to existing XML file
     * @param action Action to perform on parsed XML document
     * @param preface optional front matter to add after the XML declaration
     * but before the XML document, e.g. a doctype or comment
     */
    static void modifyXml(String xmlPath, Action<? super Node> action, String preface) throws IOException, ParserConfigurationException,
        SAXException {
        File xmlFile = new File(xmlPath);
        if (xmlFile.exists()) {
            Node xml = parseXml(xmlPath);
            action.execute(xml);

            try (PrintWriter writer = new PrintWriter(xmlFile)) {
                var printer = new XmlNodePrinter(writer);
                printer.setNamespaceAware(true);
                printer.setPreserveWhitespace(true);
                writer.write("<?xml version=\"1.0\"?>\n");
                if (preface != null) {
                    writer.write(preface);
                }
                printer.print(xml);
            }
        }
    }
}
