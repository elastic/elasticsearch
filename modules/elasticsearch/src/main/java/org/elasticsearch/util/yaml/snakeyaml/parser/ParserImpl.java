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
package org.elasticsearch.util.yaml.snakeyaml.parser;

import org.elasticsearch.util.yaml.snakeyaml.error.Mark;
import org.elasticsearch.util.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.util.yaml.snakeyaml.events.*;
import org.elasticsearch.util.yaml.snakeyaml.nodes.Tag;
import org.elasticsearch.util.yaml.snakeyaml.reader.StreamReader;
import org.elasticsearch.util.yaml.snakeyaml.scanner.Scanner;
import org.elasticsearch.util.yaml.snakeyaml.scanner.ScannerImpl;
import org.elasticsearch.util.yaml.snakeyaml.tokens.*;
import org.elasticsearch.util.yaml.snakeyaml.util.ArrayStack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 * # The following YAML grammar is LL(1) and is parsed by a recursive descent
 * parser.
 * stream            ::= STREAM-START implicit_document? explicit_document* STREAM-END
 * implicit_document ::= block_node DOCUMENT-END*
 * explicit_document ::= DIRECTIVE* DOCUMENT-START block_node? DOCUMENT-END*
 * block_node_or_indentless_sequence ::=
 *                       ALIAS
 *                       | properties (block_content | indentless_block_sequence)?
 *                       | block_content
 *                       | indentless_block_sequence
 * block_node        ::= ALIAS
 *                       | properties block_content?
 *                       | block_content
 * flow_node         ::= ALIAS
 *                       | properties flow_content?
 *                       | flow_content
 * properties        ::= TAG ANCHOR? | ANCHOR TAG?
 * block_content     ::= block_collection | flow_collection | SCALAR
 * flow_content      ::= flow_collection | SCALAR
 * block_collection  ::= block_sequence | block_mapping
 * flow_collection   ::= flow_sequence | flow_mapping
 * block_sequence    ::= BLOCK-SEQUENCE-START (BLOCK-ENTRY block_node?)* BLOCK-END
 * indentless_sequence   ::= (BLOCK-ENTRY block_node?)+
 * block_mapping     ::= BLOCK-MAPPING_START
 *                       ((KEY block_node_or_indentless_sequence?)?
 *                       (VALUE block_node_or_indentless_sequence?)?)*
 *                       BLOCK-END
 * flow_sequence     ::= FLOW-SEQUENCE-START
 *                       (flow_sequence_entry FLOW-ENTRY)*
 *                       flow_sequence_entry?
 *                       FLOW-SEQUENCE-END
 * flow_sequence_entry   ::= flow_node | KEY flow_node? (VALUE flow_node?)?
 * flow_mapping      ::= FLOW-MAPPING-START
 *                       (flow_mapping_entry FLOW-ENTRY)*
 *                       flow_mapping_entry?
 *                       FLOW-MAPPING-END
 * flow_mapping_entry    ::= flow_node | KEY flow_node? (VALUE flow_node?)?
 * FIRST sets:
 * stream: { STREAM-START }
 * explicit_document: { DIRECTIVE DOCUMENT-START }
 * implicit_document: FIRST(block_node)
 * block_node: { ALIAS TAG ANCHOR SCALAR BLOCK-SEQUENCE-START BLOCK-MAPPING-START FLOW-SEQUENCE-START FLOW-MAPPING-START }
 * flow_node: { ALIAS ANCHOR TAG SCALAR FLOW-SEQUENCE-START FLOW-MAPPING-START }
 * block_content: { BLOCK-SEQUENCE-START BLOCK-MAPPING-START FLOW-SEQUENCE-START FLOW-MAPPING-START SCALAR }
 * flow_content: { FLOW-SEQUENCE-START FLOW-MAPPING-START SCALAR }
 * block_collection: { BLOCK-SEQUENCE-START BLOCK-MAPPING-START }
 * flow_collection: { FLOW-SEQUENCE-START FLOW-MAPPING-START }
 * block_sequence: { BLOCK-SEQUENCE-START }
 * block_mapping: { BLOCK-MAPPING-START }
 * block_node_or_indentless_sequence: { ALIAS ANCHOR TAG SCALAR BLOCK-SEQUENCE-START BLOCK-MAPPING-START FLOW-SEQUENCE-START FLOW-MAPPING-START BLOCK-ENTRY }
 * indentless_sequence: { ENTRY }
 * flow_collection: { FLOW-SEQUENCE-START FLOW-MAPPING-START }
 * flow_sequence: { FLOW-SEQUENCE-START }
 * flow_mapping: { FLOW-MAPPING-START }
 * flow_sequence_entry: { ALIAS ANCHOR TAG SCALAR FLOW-SEQUENCE-START FLOW-MAPPING-START KEY }
 * flow_mapping_entry: { ALIAS ANCHOR TAG SCALAR FLOW-SEQUENCE-START FLOW-MAPPING-START KEY }
 * </pre>
 *
 * Since writing a recursive-descendant parser is a straightforward task, we do
 * not give many comments here.
 *
 * @see <a href="http://pyyaml.org/wiki/PyYAML">PyYAML</a> for more information
 */
public final class ParserImpl implements Parser {
    private static final Map<String, String> DEFAULT_TAGS = new HashMap<String, String>();

    static {
        DEFAULT_TAGS.put("!", "!");
        DEFAULT_TAGS.put("!!", Tag.PREFIX);
    }

    private final Scanner scanner;
    private Event currentEvent;
    private List<Integer> yamlVersion;
    private Map<String, String> tagHandles;
    private final ArrayStack<Production> states;
    private final ArrayStack<Mark> marks;
    private Production state;

    public ParserImpl(StreamReader reader) {
        this.scanner = new ScannerImpl(reader);
        currentEvent = null;
        yamlVersion = null;
        tagHandles = new HashMap<String, String>();
        states = new ArrayStack<Production>(100);
        marks = new ArrayStack<Mark>(10);
        state = new ParseStreamStart();
    }

    /**
     * Check the type of the next event.
     */
    public boolean checkEvent(Event.ID choices) {
        peekEvent();
        if (currentEvent != null) {
            if (currentEvent.is(choices)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Get the next event.
     */

    public Event peekEvent() {
        if (currentEvent == null) {
            if (state != null) {
                currentEvent = state.produce();
            }
        }
        return currentEvent;
    }

    /*
     * Get the next event and proceed further.
     */

    public Event getEvent() {
        peekEvent();
        Event value = currentEvent;
        currentEvent = null;
        return value;
    }

    /**
     * <pre>
     * stream    ::= STREAM-START implicit_document? explicit_document* STREAM-END
     * implicit_document ::= block_node DOCUMENT-END*
     * explicit_document ::= DIRECTIVE* DOCUMENT-START block_node? DOCUMENT-END*
     * </pre>
     */
    private class ParseStreamStart implements Production {
        public Event produce() {
            // Parse the stream start.
            StreamStartToken token = (StreamStartToken) scanner.getToken();
            Event event = new StreamStartEvent(token.getStartMark(), token.getEndMark());
            // Prepare the next state.
            state = new ParseImplicitDocumentStart();
            return event;
        }
    }

    private class ParseImplicitDocumentStart implements Production {
        public Event produce() {
            // Parse an implicit document.
            if (!scanner.checkToken(Token.ID.Directive, Token.ID.DocumentStart, Token.ID.StreamEnd)) {
                tagHandles = DEFAULT_TAGS;
                Token token = scanner.peekToken();
                Mark startMark = token.getStartMark();
                Mark endMark = startMark;
                Event event = new DocumentStartEvent(startMark, endMark, false, null, null);
                // Prepare the next state.
                states.push(new ParseDocumentEnd());
                state = new ParseBlockNode();
                return event;
            } else {
                Production p = new ParseDocumentStart();
                return p.produce();
            }
        }
    }

    private class ParseDocumentStart implements Production {
        @SuppressWarnings("unchecked")
        public Event produce() {
            // Parse any extra document end indicators.
            while (scanner.checkToken(Token.ID.DocumentEnd)) {
                scanner.getToken();
            }
            // Parse an explicit document.
            Event event;
            if (!scanner.checkToken(Token.ID.StreamEnd)) {
                Token token = scanner.peekToken();
                Mark startMark = token.getStartMark();
                List<Object> version_tags = processDirectives();
                List<Object> version = (List<Object>) version_tags.get(0);
                Map<String, String> tags = (Map<String, String>) version_tags.get(1);
                if (!scanner.checkToken(Token.ID.DocumentStart)) {
                    throw new ParserException(null, null, "expected '<document start>', but found "
                            + scanner.peekToken().getTokenId(), scanner.peekToken().getStartMark());
                }
                token = scanner.getToken();
                Mark endMark = token.getEndMark();
                Integer[] versionInteger;
                if (version != null) {
                    versionInteger = new Integer[2];
                    versionInteger = version.toArray(versionInteger);
                } else {
                    versionInteger = null;
                }
                event = new DocumentStartEvent(startMark, endMark, true, versionInteger, tags);
                states.push(new ParseDocumentEnd());
                state = new ParseDocumentContent();
            } else {
                // Parse the end of the stream.
                StreamEndToken token = (StreamEndToken) scanner.getToken();
                event = new StreamEndEvent(token.getStartMark(), token.getEndMark());
                if (!states.isEmpty()) {
                    throw new YAMLException("Unexpected end of stream. States left: " + states);
                }
                if (!marks.isEmpty()) {
                    throw new YAMLException("Unexpected end of stream. Marks left: " + marks);
                }
                state = null;
            }
            return event;
        }
    }

    private class ParseDocumentEnd implements Production {
        public Event produce() {
            // Parse the document end.
            Token token = scanner.peekToken();
            Mark startMark = token.getStartMark();
            Mark endMark = startMark;
            boolean explicit = false;
            if (scanner.checkToken(Token.ID.DocumentEnd)) {
                token = scanner.getToken();
                endMark = token.getEndMark();
                explicit = true;
            }
            Event event = new DocumentEndEvent(startMark, endMark, explicit);
            // Prepare the next state.
            state = new ParseDocumentStart();
            return event;
        }
    }

    private class ParseDocumentContent implements Production {
        public Event produce() {
            Event event;
            if (scanner.checkToken(Token.ID.Directive, Token.ID.DocumentStart,
                    Token.ID.DocumentEnd, Token.ID.StreamEnd)) {
                event = processEmptyScalar(scanner.peekToken().getStartMark());
                state = states.pop();
                return event;
            } else {
                Production p = new ParseBlockNode();
                return p.produce();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<Object> processDirectives() {
        yamlVersion = null;
        tagHandles = new HashMap<String, String>();
        while (scanner.checkToken(Token.ID.Directive)) {
            DirectiveToken token = (DirectiveToken) scanner.getToken();
            if (token.getName().equals("YAML")) {
                if (yamlVersion != null) {
                    throw new ParserException(null, null, "found duplicate YAML directive", token
                            .getStartMark());
                }
                List<Integer> value = (List<Integer>) token.getValue();
                Integer major = value.get(0);
                if (major != 1) {
                    throw new ParserException(null, null,
                            "found incompatible YAML document (version 1.* is required)", token
                                    .getStartMark());
                }
                yamlVersion = (List<Integer>) token.getValue();
            } else if (token.getName().equals("TAG")) {
                List<String> value = (List<String>) token.getValue();
                String handle = value.get(0);
                String prefix = value.get(1);
                if (tagHandles.containsKey(handle)) {
                    throw new ParserException(null, null, "duplicate tag handle " + handle, token
                            .getStartMark());
                }
                tagHandles.put(handle, prefix);
            }
        }
        List<Object> value = new ArrayList<Object>(2);
        value.add(yamlVersion);
        if (!tagHandles.isEmpty()) {
            value.add(new HashMap<String, String>(tagHandles));
        } else {
            value.add(new HashMap<String, String>());
        }
        for (String key : DEFAULT_TAGS.keySet()) {
            if (!tagHandles.containsKey(key)) {
                tagHandles.put(key, DEFAULT_TAGS.get(key));
            }
        }
        return value;
    }

    /**
     * <pre>
     *  block_node_or_indentless_sequence ::= ALIAS
     *                | properties (block_content | indentless_block_sequence)?
     *                | block_content
     *                | indentless_block_sequence
     *  block_node    ::= ALIAS
     *                    | properties block_content?
     *                    | block_content
     *  flow_node     ::= ALIAS
     *                    | properties flow_content?
     *                    | flow_content
     *  properties    ::= TAG ANCHOR? | ANCHOR TAG?
     *  block_content     ::= block_collection | flow_collection | SCALAR
     *  flow_content      ::= flow_collection | SCALAR
     *  block_collection  ::= block_sequence | block_mapping
     *  flow_collection   ::= flow_sequence | flow_mapping
     * </pre>
     */

    private class ParseBlockNode implements Production {
        public Event produce() {
            return parseNode(true, false);
        }
    }

    private Event parseFlowNode() {
        return parseNode(false, false);
    }

    private Event parseBlockNodeOrIndentlessSequence() {
        return parseNode(true, true);
    }

    private Event parseNode(boolean block, boolean indentlessSequence) {
        Event event;
        Mark startMark = null;
        Mark endMark = null;
        Mark tagMark = null;
        if (scanner.checkToken(Token.ID.Alias)) {
            AliasToken token = (AliasToken) scanner.getToken();
            event = new AliasEvent(token.getValue(), token.getStartMark(), token.getEndMark());
            state = states.pop();
        } else {
            String anchor = null;
            TagTuple tagTokenTag = null;
            if (scanner.checkToken(Token.ID.Anchor)) {
                AnchorToken token = (AnchorToken) scanner.getToken();
                startMark = token.getStartMark();
                endMark = token.getEndMark();
                anchor = token.getValue();
                if (scanner.checkToken(Token.ID.Tag)) {
                    TagToken tagToken = (TagToken) scanner.getToken();
                    tagMark = tagToken.getStartMark();
                    endMark = tagToken.getEndMark();
                    tagTokenTag = tagToken.getValue();
                }
            } else if (scanner.checkToken(Token.ID.Tag)) {
                TagToken tagToken = (TagToken) scanner.getToken();
                startMark = tagToken.getStartMark();
                tagMark = startMark;
                endMark = tagToken.getEndMark();
                tagTokenTag = tagToken.getValue();
                if (scanner.checkToken(Token.ID.Anchor)) {
                    AnchorToken token = (AnchorToken) scanner.getToken();
                    endMark = token.getEndMark();
                    anchor = token.getValue();
                }
            }
            String tag = null;
            if (tagTokenTag != null) {
                String handle = tagTokenTag.getHandle();
                String suffix = tagTokenTag.getSuffix();
                if (handle != null) {
                    if (!tagHandles.containsKey(handle)) {
                        throw new ParserException("while parsing a node", startMark,
                                "found undefined tag handle " + handle, tagMark);
                    }
                    tag = tagHandles.get(handle) + suffix;
                } else {
                    tag = suffix;
                }
            }
            if (startMark == null) {
                startMark = scanner.peekToken().getStartMark();
                endMark = startMark;
            }
            event = null;
            boolean implicit = (tag == null || tag.equals("!"));
            if (indentlessSequence && scanner.checkToken(Token.ID.BlockEntry)) {
                endMark = scanner.peekToken().getEndMark();
                event = new SequenceStartEvent(anchor, tag, implicit, startMark, endMark,
                        Boolean.FALSE);
                state = new ParseIndentlessSequenceEntry();
            } else {
                if (scanner.checkToken(Token.ID.Scalar)) {
                    ScalarToken token = (ScalarToken) scanner.getToken();
                    endMark = token.getEndMark();
                    ImplicitTuple implicitValues;
                    if ((token.getPlain() && tag == null) || "!".equals(tag)) {
                        implicitValues = new ImplicitTuple(true, false);
                    } else if (tag == null) {
                        implicitValues = new ImplicitTuple(false, true);
                    } else {
                        implicitValues = new ImplicitTuple(false, false);
                    }
                    event = new ScalarEvent(anchor, tag, implicitValues, token.getValue(),
                            startMark, endMark, token.getStyle());
                    state = states.pop();
                } else if (scanner.checkToken(Token.ID.FlowSequenceStart)) {
                    endMark = scanner.peekToken().getEndMark();
                    event = new SequenceStartEvent(anchor, tag, implicit, startMark, endMark,
                            Boolean.TRUE);
                    state = new ParseFlowSequenceFirstEntry();
                } else if (scanner.checkToken(Token.ID.FlowMappingStart)) {
                    endMark = scanner.peekToken().getEndMark();
                    event = new MappingStartEvent(anchor, tag, implicit, startMark, endMark,
                            Boolean.TRUE);
                    state = new ParseFlowMappingFirstKey();
                } else if (block && scanner.checkToken(Token.ID.BlockSequenceStart)) {
                    endMark = scanner.peekToken().getStartMark();
                    event = new SequenceStartEvent(anchor, tag, implicit, startMark, endMark,
                            Boolean.FALSE);
                    state = new ParseBlockSequenceFirstEntry();
                } else if (block && scanner.checkToken(Token.ID.BlockMappingStart)) {
                    endMark = scanner.peekToken().getStartMark();
                    event = new MappingStartEvent(anchor, tag, implicit, startMark, endMark,
                            Boolean.FALSE);
                    state = new ParseBlockMappingFirstKey();
                } else if (anchor != null || tag != null) {
                    // Empty scalars are allowed even if a tag or an anchor is
                    // specified.
                    event = new ScalarEvent(anchor, tag, new ImplicitTuple(implicit, false), "",
                            startMark, endMark, (char) 0);
                    state = states.pop();
                } else {
                    String node;
                    if (block) {
                        node = "block";
                    } else {
                        node = "flow";
                    }
                    Token token = scanner.peekToken();
                    throw new ParserException("while parsing a " + node + " node", startMark,
                            "expected the node content, but found " + token.getTokenId(), token
                                    .getStartMark());
                }
            }
        }
        return event;
    }

    // block_sequence ::= BLOCK-SEQUENCE-START (BLOCK-ENTRY block_node?)*
    // BLOCK-END

    private class ParseBlockSequenceFirstEntry implements Production {
        public Event produce() {
            Token token = scanner.getToken();
            marks.push(token.getStartMark());
            return new ParseBlockSequenceEntry().produce();
        }
    }

    private class ParseBlockSequenceEntry implements Production {
        public Event produce() {
            if (scanner.checkToken(Token.ID.BlockEntry)) {
                BlockEntryToken token = (BlockEntryToken) scanner.getToken();
                if (!scanner.checkToken(Token.ID.BlockEntry, Token.ID.BlockEnd)) {
                    states.push(new ParseBlockSequenceEntry());
                    return new ParseBlockNode().produce();
                } else {
                    state = new ParseBlockSequenceEntry();
                    return processEmptyScalar(token.getEndMark());
                }
            }
            if (!scanner.checkToken(Token.ID.BlockEnd)) {
                Token token = scanner.peekToken();
                throw new ParserException("while parsing a block collection", marks.pop(),
                        "expected <block end>, but found " + token.getTokenId(), token
                                .getStartMark());
            }
            Token token = scanner.getToken();
            Event event = new SequenceEndEvent(token.getStartMark(), token.getEndMark());
            state = states.pop();
            marks.pop();
            return event;
        }
    }

    // indentless_sequence ::= (BLOCK-ENTRY block_node?)+

    private class ParseIndentlessSequenceEntry implements Production {
        public Event produce() {
            if (scanner.checkToken(Token.ID.BlockEntry)) {
                Token token = scanner.getToken();
                if (!scanner.checkToken(Token.ID.BlockEntry, Token.ID.Key, Token.ID.Value,
                        Token.ID.BlockEnd)) {
                    states.push(new ParseIndentlessSequenceEntry());
                    return new ParseBlockNode().produce();
                } else {
                    state = new ParseIndentlessSequenceEntry();
                    return processEmptyScalar(token.getEndMark());
                }
            }
            Token token = scanner.peekToken();
            Event event = new SequenceEndEvent(token.getStartMark(), token.getEndMark());
            state = states.pop();
            return event;
        }
    }

    private class ParseBlockMappingFirstKey implements Production {
        public Event produce() {
            Token token = scanner.getToken();
            marks.push(token.getStartMark());
            return new ParseBlockMappingKey().produce();
        }
    }

    private class ParseBlockMappingKey implements Production {
        public Event produce() {
            if (scanner.checkToken(Token.ID.Key)) {
                Token token = scanner.getToken();
                if (!scanner.checkToken(Token.ID.Key, Token.ID.Value, Token.ID.BlockEnd)) {
                    states.push(new ParseBlockMappingValue());
                    return parseBlockNodeOrIndentlessSequence();
                } else {
                    state = new ParseBlockMappingValue();
                    return processEmptyScalar(token.getEndMark());
                }
            }
            if (!scanner.checkToken(Token.ID.BlockEnd)) {
                Token token = scanner.peekToken();
                throw new ParserException("while parsing a block mapping", marks.pop(),
                        "expected <block end>, but found " + token.getTokenId(), token
                                .getStartMark());
            }
            Token token = scanner.getToken();
            Event event = new MappingEndEvent(token.getStartMark(), token.getEndMark());
            state = states.pop();
            marks.pop();
            return event;
        }
    }

    private class ParseBlockMappingValue implements Production {
        public Event produce() {
            if (scanner.checkToken(Token.ID.Value)) {
                Token token = scanner.getToken();
                if (!scanner.checkToken(Token.ID.Key, Token.ID.Value, Token.ID.BlockEnd)) {
                    states.push(new ParseBlockMappingKey());
                    return parseBlockNodeOrIndentlessSequence();
                } else {
                    state = new ParseBlockMappingKey();
                    return processEmptyScalar(token.getEndMark());
                }
            }
            state = new ParseBlockMappingKey();
            Token token = scanner.peekToken();
            return processEmptyScalar(token.getStartMark());
        }
    }

    /**
     * <pre>
     * flow_sequence     ::= FLOW-SEQUENCE-START
     *                       (flow_sequence_entry FLOW-ENTRY)*
     *                       flow_sequence_entry?
     *                       FLOW-SEQUENCE-END
     * flow_sequence_entry   ::= flow_node | KEY flow_node? (VALUE flow_node?)?
     * Note that while production rules for both flow_sequence_entry and
     * flow_mapping_entry are equal, their interpretations are different.
     * For `flow_sequence_entry`, the part `KEY flow_node? (VALUE flow_node?)?`
     * generate an inline mapping (set syntax).
     * </pre>
     */
    private class ParseFlowSequenceFirstEntry implements Production {
        public Event produce() {
            Token token = scanner.getToken();
            marks.push(token.getStartMark());
            return new ParseFlowSequenceEntry(true).produce();
        }
    }

    private class ParseFlowSequenceEntry implements Production {
        private boolean first = false;

        public ParseFlowSequenceEntry(boolean first) {
            this.first = first;
        }

        public Event produce() {
            if (!scanner.checkToken(Token.ID.FlowSequenceEnd)) {
                if (!first) {
                    if (scanner.checkToken(Token.ID.FlowEntry)) {
                        scanner.getToken();
                    } else {
                        Token token = scanner.peekToken();
                        throw new ParserException("while parsing a flow sequence", marks.pop(),
                                "expected ',' or ']', but got " + token.getTokenId(), token
                                        .getStartMark());
                    }
                }
                if (scanner.checkToken(Token.ID.Key)) {
                    Token token = scanner.peekToken();
                    Event event = new MappingStartEvent(null, null, true, token.getStartMark(),
                            token.getEndMark(), Boolean.TRUE);
                    state = new ParseFlowSequenceEntryMappingKey();
                    return event;
                } else if (!scanner.checkToken(Token.ID.FlowSequenceEnd)) {
                    states.push(new ParseFlowSequenceEntry(false));
                    return parseFlowNode();
                }
            }
            Token token = scanner.getToken();
            Event event = new SequenceEndEvent(token.getStartMark(), token.getEndMark());
            state = states.pop();
            marks.pop();
            return event;
        }
    }

    private class ParseFlowSequenceEntryMappingKey implements Production {
        public Event produce() {
            Token token = scanner.getToken();
            if (!scanner.checkToken(Token.ID.Value, Token.ID.FlowEntry, Token.ID.FlowSequenceEnd)) {
                states.push(new ParseFlowSequenceEntryMappingValue());
                return parseFlowNode();
            } else {
                state = new ParseFlowSequenceEntryMappingValue();
                return processEmptyScalar(token.getEndMark());
            }
        }
    }

    private class ParseFlowSequenceEntryMappingValue implements Production {
        public Event produce() {
            if (scanner.checkToken(Token.ID.Value)) {
                Token token = scanner.getToken();
                if (!scanner.checkToken(Token.ID.FlowEntry, Token.ID.FlowSequenceEnd)) {
                    states.push(new ParseFlowSequenceEntryMappingEnd());
                    return parseFlowNode();
                } else {
                    state = new ParseFlowSequenceEntryMappingEnd();
                    return processEmptyScalar(token.getEndMark());
                }
            } else {
                state = new ParseFlowSequenceEntryMappingEnd();
                Token token = scanner.peekToken();
                return processEmptyScalar(token.getStartMark());
            }
        }
    }

    private class ParseFlowSequenceEntryMappingEnd implements Production {
        public Event produce() {
            state = new ParseFlowSequenceEntry(false);
            Token token = scanner.peekToken();
            return new MappingEndEvent(token.getStartMark(), token.getEndMark());
        }
    }

    /**
     * <pre>
     *   flow_mapping  ::= FLOW-MAPPING-START
     *          (flow_mapping_entry FLOW-ENTRY)*
     *          flow_mapping_entry?
     *          FLOW-MAPPING-END
     *   flow_mapping_entry    ::= flow_node | KEY flow_node? (VALUE flow_node?)?
     * </pre>
     */
    private class ParseFlowMappingFirstKey implements Production {
        public Event produce() {
            Token token = scanner.getToken();
            marks.push(token.getStartMark());
            return new ParseFlowMappingKey(true).produce();
        }
    }

    private class ParseFlowMappingKey implements Production {
        private boolean first = false;

        public ParseFlowMappingKey(boolean first) {
            this.first = first;
        }

        public Event produce() {
            if (!scanner.checkToken(Token.ID.FlowMappingEnd)) {
                if (!first) {
                    if (scanner.checkToken(Token.ID.FlowEntry)) {
                        scanner.getToken();
                    } else {
                        Token token = scanner.peekToken();
                        throw new ParserException("while parsing a flow mapping", marks.pop(),
                                "expected ',' or '}', but got " + token.getTokenId(), token
                                        .getStartMark());
                    }
                }
                if (scanner.checkToken(Token.ID.Key)) {
                    Token token = scanner.getToken();
                    if (!scanner.checkToken(Token.ID.Value, Token.ID.FlowEntry,
                            Token.ID.FlowMappingEnd)) {
                        states.push(new ParseFlowMappingValue());
                        return parseFlowNode();
                    } else {
                        state = new ParseFlowMappingValue();
                        return processEmptyScalar(token.getEndMark());
                    }
                } else if (!scanner.checkToken(Token.ID.FlowMappingEnd)) {
                    states.push(new ParseFlowMappingEmptyValue());
                    return parseFlowNode();
                }
            }
            Token token = scanner.getToken();
            Event event = new MappingEndEvent(token.getStartMark(), token.getEndMark());
            state = states.pop();
            marks.pop();
            return event;
        }
    }

    private class ParseFlowMappingValue implements Production {
        public Event produce() {
            if (scanner.checkToken(Token.ID.Value)) {
                Token token = scanner.getToken();
                if (!scanner.checkToken(Token.ID.FlowEntry, Token.ID.FlowMappingEnd)) {
                    states.push(new ParseFlowMappingKey(false));
                    return parseFlowNode();
                } else {
                    state = new ParseFlowMappingKey(false);
                    return processEmptyScalar(token.getEndMark());
                }
            } else {
                state = new ParseFlowMappingKey(false);
                Token token = scanner.peekToken();
                return processEmptyScalar(token.getStartMark());
            }
        }
    }

    private class ParseFlowMappingEmptyValue implements Production {
        public Event produce() {
            state = new ParseFlowMappingKey(false);
            return processEmptyScalar(scanner.peekToken().getStartMark());
        }
    }

    /**
     * <pre>
     * block_mapping     ::= BLOCK-MAPPING_START
     *           ((KEY block_node_or_indentless_sequence?)?
     *           (VALUE block_node_or_indentless_sequence?)?)*
     *           BLOCK-END
     * </pre>
     */
    private Event processEmptyScalar(Mark mark) {
        return new ScalarEvent(null, null, new ImplicitTuple(true, false), "", mark, mark, (char) 0);
    }
}
