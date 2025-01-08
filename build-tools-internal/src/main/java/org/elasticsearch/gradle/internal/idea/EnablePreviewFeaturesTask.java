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

import groovy.util.NodeList;

import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;

public class EnablePreviewFeaturesTask extends DefaultTask {

    public void enablePreview(String moduleFile, String languageLevel) throws IOException, ParserConfigurationException, SAXException {
        IdeaXmlUtil.modifyXml(moduleFile, xml -> {
            // Find the 'component' node
            NodeList nodes = (NodeList) xml.depthFirst();
            Node componentNode = null;
            for (Object node : nodes) {
                Node currentNode = (Node) node;
                if ("component".equals(currentNode.name()) && "NewModuleRootManager".equals(currentNode.attribute("name"))) {
                    componentNode = currentNode;
                    break;
                }
            }

            // Add the attribute to the 'component' node
            if (componentNode != null) {
                componentNode.attributes().put("LANGUAGE_LEVEL", languageLevel);
            }
        });
    }
}
