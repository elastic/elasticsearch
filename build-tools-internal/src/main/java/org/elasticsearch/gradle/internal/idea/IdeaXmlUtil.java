/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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

    static void modifyXml(String xmlPath, Action<? super Node> action) throws IOException, ParserConfigurationException, SAXException {
        modifyXml(xmlPath, action, null);
    }

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
