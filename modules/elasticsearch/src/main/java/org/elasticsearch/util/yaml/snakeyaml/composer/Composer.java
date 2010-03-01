/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.util.yaml.snakeyaml.composer;

import org.elasticsearch.util.yaml.snakeyaml.events.*;
import org.elasticsearch.util.yaml.snakeyaml.nodes.*;
import org.elasticsearch.util.yaml.snakeyaml.parser.Parser;
import org.elasticsearch.util.yaml.snakeyaml.resolver.Resolver;

import java.util.*;

/**
 * Creates a node graph from parser events.
 * <p>
 * Corresponds to the 'Compose' step as described in chapter 3.1 of the <a
 * href="http://yaml.org/spec/1.1/">YAML Specification</a>.
 * </p>
 */
public class Composer {
    private final Parser parser;
    private final Resolver resolver;
    private final Map<String, Node> anchors;
    private final Set<Node> recursiveNodes;

    public Composer(Parser parser, Resolver resolver) {
        this.parser = parser;
        this.resolver = resolver;
        this.anchors = new HashMap<String, Node>();
        this.recursiveNodes = new HashSet<Node>();
    }

    /**
     * Checks if further documents are available.
     *
     * @return <code>true</code> if there is at least one more document.
     */
    public boolean checkNode() {
        // Drop the STREAM-START event.
        if (parser.checkEvent(Event.ID.StreamStart)) {
            parser.getEvent();
        }
        // If there are more documents available?
        return !parser.checkEvent(Event.ID.StreamEnd);
    }

    /**
     * Reads and composes the next document.
     *
     * @return The root node of the document or <code>null</code> if no more
     *         documents are available.
     */
    public Node getNode() {
        // Get the root node of the next document.
        if (!parser.checkEvent(Event.ID.StreamEnd)) {
            return composeDocument();
        } else {
            return (Node) null;
        }
    }

    /**
     * Reads a document from a source that contains only one document.
     * <p>
     * If the stream contains more than one document an exception is thrown.
     * </p>
     *
     * @return The root node of the document or <code>null</code> if no document
     *         is available.
     */
    public Node getSingleNode() {
        // Drop the STREAM-START event.
        parser.getEvent();
        // Compose a document if the stream is not empty.
        Node document = null;
        if (!parser.checkEvent(Event.ID.StreamEnd)) {
            document = composeDocument();
        }
        // Ensure that the stream contains no more documents.
        if (!parser.checkEvent(Event.ID.StreamEnd)) {
            Event event = parser.getEvent();
            throw new ComposerException("expected a single document in the stream", document
                    .getStartMark(), "but found another document", event.getStartMark());
        }
        // Drop the STREAM-END event.
        parser.getEvent();
        return document;
    }

    private Node composeDocument() {
        // Drop the DOCUMENT-START event.
        parser.getEvent();
        // Compose the root node.
        Node node = composeNode(null, null);
        // Drop the DOCUMENT-END event.
        parser.getEvent();
        this.anchors.clear();
        recursiveNodes.clear();
        return node;
    }

    private Node composeNode(Node parent, Object index) {
        recursiveNodes.add(parent);
        if (parser.checkEvent(Event.ID.Alias)) {
            AliasEvent event = (AliasEvent) parser.getEvent();
            String anchor = event.getAnchor();
            if (!anchors.containsKey(anchor)) {
                throw new ComposerException(null, null, "found undefined alias " + anchor, event
                        .getStartMark());
            }
            Node result = (Node) anchors.get(anchor);
            if (recursiveNodes.remove(result)) {
                result.setTwoStepsConstruction(true);
            }
            return result;
        }
        NodeEvent event = (NodeEvent) parser.peekEvent();
        String anchor = null;
        anchor = event.getAnchor();
        if (anchor != null && anchors.containsKey(anchor)) {
            throw new ComposerException("found duplicate anchor " + anchor + "; first occurence",
                    this.anchors.get(anchor).getStartMark(), "second occurence", event
                            .getStartMark());
        }
        // resolver.descendResolver(parent, index);
        Node node = null;
        if (parser.checkEvent(Event.ID.Scalar)) {
            node = composeScalarNode(anchor);
        } else if (parser.checkEvent(Event.ID.SequenceStart)) {
            node = composeSequenceNode(anchor);
        } else {
            node = composeMappingNode(anchor);
        }
        // resolver.ascendResolver();
        recursiveNodes.remove(parent);
        return node;
    }

    private Node composeScalarNode(String anchor) {
        ScalarEvent ev = (ScalarEvent) parser.getEvent();
        String tag = ev.getTag();
        boolean resolved = false;
        Tag nodeTag;
        if (tag == null || tag.equals("!")) {
            nodeTag = resolver.resolve(NodeId.scalar, ev.getValue(), ev.getImplicit().isFirst());
            resolved = true;
        } else {
            nodeTag = new Tag(tag);
        }
        Node node = new ScalarNode(nodeTag, resolved, ev.getValue(), ev.getStartMark(), ev
                .getEndMark(), ev.getStyle());
        if (anchor != null) {
            anchors.put(anchor, node);
        }
        return node;
    }

    private Node composeSequenceNode(String anchor) {
        SequenceStartEvent startEvent = (SequenceStartEvent) parser.getEvent();
        String tag = startEvent.getTag();
        Tag nodeTag;
        boolean resolved = false;
        if (tag == null || tag.equals("!")) {
            nodeTag = resolver.resolve(NodeId.sequence, null, startEvent.getImplicit());
            resolved = true;
        } else {
            nodeTag = new Tag(tag);
        }
        SequenceNode node = new SequenceNode(nodeTag, resolved, new ArrayList<Node>(), startEvent
                .getStartMark(), null, startEvent.getFlowStyle());
        if (anchor != null) {
            anchors.put(anchor, node);
        }
        int index = 0;
        while (!parser.checkEvent(Event.ID.SequenceEnd)) {
            (node.getValue()).add(composeNode(node, index));
            index++;
        }
        Event endEvent = parser.getEvent();
        node.setEndMark(endEvent.getEndMark());
        return node;
    }

    private Node composeMappingNode(String anchor) {
        MappingStartEvent startEvent = (MappingStartEvent) parser.getEvent();
        String tag = startEvent.getTag();
        Tag nodeTag;
        boolean resolved = false;
        if (tag == null || tag.equals("!")) {
            nodeTag = resolver.resolve(NodeId.mapping, null, startEvent.getImplicit());
            resolved = true;
        } else {
            nodeTag = new Tag(tag);
        }
        MappingNode node = new MappingNode(nodeTag, resolved, new ArrayList<NodeTuple>(),
                startEvent.getStartMark(), null, startEvent.getFlowStyle());
        if (anchor != null) {
            anchors.put(anchor, node);
        }
        while (!parser.checkEvent(Event.ID.MappingEnd)) {
            Node itemKey = composeNode(node, null);
            Node itemValue = composeNode(node, itemKey);
            node.getValue().add(new NodeTuple(itemKey, itemValue));
        }
        Event endEvent = parser.getEvent();
        node.setEndMark(endEvent.getEndMark());
        return node;
    }
}
