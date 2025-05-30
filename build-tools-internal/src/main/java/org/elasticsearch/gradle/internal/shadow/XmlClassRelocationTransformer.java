/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.shadow;

//import com.github.jengelman.gradle.plugins.shadow.ShadowStats;

import com.github.jengelman.gradle.plugins.shadow.relocation.RelocateClassContext;
import com.github.jengelman.gradle.plugins.shadow.relocation.Relocator;
import com.github.jengelman.gradle.plugins.shadow.transformers.ResourceTransformer;
import com.github.jengelman.gradle.plugins.shadow.transformers.TransformerContext;

import org.apache.commons.io.IOUtils;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;
import org.gradle.api.file.FileTreeElement;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class XmlClassRelocationTransformer implements ResourceTransformer {

    boolean hasTransformedResource = false;

    private Document doc;

    private String resource;

    @Override
    public boolean canTransformResource(FileTreeElement element) {
        String path = element.getRelativePath().getPathString();
        if (resource != null && resource.equals(path)) {
            return true;
        }
        return false;
    }

    @Override
    public void transform(TransformerContext context) {
        try {
            BufferedInputStream bis = new BufferedInputStream(context.getInputStream());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(bis);
            doc.getDocumentElement().normalize();
            Node root = doc.getDocumentElement();
            walkThroughNodes(root, context);
            if (hasTransformedResource == false) {
                this.doc = null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Error parsing xml file in " + context.getInputStream(), e);
        }
    }

    private static String getRelocatedClass(String className, TransformerContext context) {
        Set<Relocator> relocators = context.getRelocators();
        if (className != null && className.length() > 0 && relocators != null) {
            for (Relocator relocator : relocators) {
                if (relocator.canRelocateClass(className)) {
                    RelocateClassContext relocateClassContext = new RelocateClassContext(className);
                    return relocator.relocateClass(relocateClassContext);
                }
            }
        }

        return className;
    }

    private void walkThroughNodes(Node node, TransformerContext context) {
        if (node.getNodeType() == Node.TEXT_NODE) {
            String nodeValue = node.getNodeValue();
            if (nodeValue.isBlank() == false) {
                String relocatedClass = getRelocatedClass(nodeValue, context);
                if (relocatedClass.equals(nodeValue) == false) {
                    node.setNodeValue(relocatedClass);
                    hasTransformedResource = true;
                }
            }
        }
        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node currentNode = nodeList.item(i);
            walkThroughNodes(currentNode, context);
        }
    }

    @Override
    public boolean hasTransformedResource() {
        return hasTransformedResource;
    }

    @Override
    public void modifyOutputStream(ZipOutputStream os, boolean preserveFileTimestamps) {
        ZipEntry entry = new ZipEntry(resource);
        if (preserveFileTimestamps) {
            entry.setTime(entry.getTime());
        }

        try {
            // Write the content back to the XML file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            DOMSource source = new DOMSource(doc);

            // Result stream will be a ByteArrayOutputStream
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            StreamResult result = new StreamResult(baos);
            // Do the transformation and serialization
            transformerFactory.newTransformer().transform(source, result);
            os.putNextEntry(entry);
            IOUtils.write(baos.toByteArray(), os);
            os.closeEntry();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TransformerException e) {
            throw new RuntimeException(e);
        } finally {
            hasTransformedResource = false;
            doc = null;
        }
    }
}
