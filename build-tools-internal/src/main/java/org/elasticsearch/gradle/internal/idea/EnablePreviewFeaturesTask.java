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
import groovy.util.NodeList;

import org.gradle.api.DefaultTask;
import org.xml.sax.SAXException;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

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
