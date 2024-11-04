/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class SyntheticSourceDocumentParserListener implements DocumentParserListener {
    private final MappingLookup mappingLookup;
    private final List<IgnoredSourceFieldMapper.NameValue> valuesToStore;

    private State state;

    public SyntheticSourceDocumentParserListener(MappingLookup mappingLookup) {
        this.mappingLookup = mappingLookup;
        this.valuesToStore = new ArrayList<>();

        this.state = new Watching(mappingLookup.getMapping().getRoot());
    }

    @Override
    public void consume(Token token) throws IOException {
        if (token == null) {
            return;
        }

        this.state = state.consume(token);
    }

    @Override
    public void consume(Event event) {
        if (event == null) {
            return;
        }

        this.state = state.consume(event);
    }

    public List<IgnoredSourceFieldMapper.NameValue> getValuesToStore() {
        return valuesToStore;
    }

    interface State {
        State consume(Token token) throws IOException;

        State consume(Event event);
    }

    class Storing implements State {
        private final State returnState;
        private final Token startingToken;
        private final String fullPath;
        private final ObjectMapper parentMapper;
        private final LuceneDocument document;

        private final XContentBuilder data;

        private int depth;

        Storing(State returnState, Token startingToken, String fullPath, ObjectMapper parentMapper, LuceneDocument document)
            throws IOException {
            this.returnState = returnState;
            this.startingToken = startingToken;
            this.fullPath = fullPath;
            this.parentMapper = parentMapper;
            this.document = document;

            // TODO use actual value from initial parser (add a "document start" token with metadata?)
            this.data = XContentBuilder.builder(XContentType.JSON.xContent());

            this.depth = 0;

            consume(startingToken);
        }

        public State consume(Token token) throws IOException {
            switch (token) {
                case Token.StartObject startObject -> {
                    data.startObject();
                    depth += 1;
                }
                case Token.EndObject endObject -> {
                    data.endObject();

                    if (processEndObjectOrArray()) {
                        return returnState;
                    }
                }
                case Token.StartArray startArray -> {
                    data.startArray();
                    depth += 1;
                }
                case Token.EndArray endArray -> {
                    data.endArray();

                    if (processEndObjectOrArray()) {
                        return returnState;
                    }
                }
                case Token.FieldName fieldName -> data.field(fieldName.name());
                case Token.StringValue stringValue -> data.value(stringValue.value());
                case Token.BooleanValue booleanValue -> data.value(booleanValue.value());
                case Token.IntValue intValue -> data.value(intValue.value());
                case Token.LongValue longValue -> data.value(longValue.value());
                case Token.BigIntegerValue bigIntegerValue -> data.value(bigIntegerValue.value());
                case Token.DoubleValue doubleValue -> data.value(doubleValue.value());
                case Token.FloatValue floatValue -> data.value(floatValue.value());
                case Token.NullValue nullValue -> data.nullValue();
            }

            return this;
        }

        public State consume(Event event) {
            // We don't expect ignored source values to span multiple documents
            assert event instanceof Event.DocumentSwitch == false : "Lucene document was changed while storing ignored source value";
            return this;
        }

        private boolean processEndObjectOrArray() throws IOException {
            depth -= 1;
            if (depth == 0) {
                var parentOffset = parentMapper.isRoot() ? 0 : parentMapper.fullPath().length() + 1;
                // TODO the way we store values is not final, maybe we should put them directly in DocumentParserContext ?
                // does not feel great though
                valuesToStore.add(
                    new IgnoredSourceFieldMapper.NameValue(fullPath, parentOffset, XContentDataHelper.encodeXContentBuilder(data), document)
                );

                return true;
            }

            return false;
        }
    }

    class Watching implements State {
        private final Stack<Parent> parents;
        private final Stack<Document> documents;

        private String fullPath;
        private Mapper currentMapper;
        private int depth;

        Watching(RootObjectMapper rootMapper) {
            this.parents = new Stack<>() {
                {
                    push(new Watching.Parent(rootMapper, 0));
                }
            };
            this.documents = new Stack<>();

            this.fullPath = MapperService.SINGLE_MAPPING_NAME;
            this.currentMapper = rootMapper;
            this.depth = 0;
        }

        public State consume(Token token) throws IOException {
            switch (token) {
                case Token.StartObject startObject -> {
                    if (currentMapper instanceof ObjectMapper om && om.isEnabled() == false) {
                        var storingState = new Storing(
                            this,
                            startObject,
                            fullPath,
                            parents.peek().parentMapper(),
                            documents.peek().document()
                        );
                        // TODO should we some cleaner "reset()" method on Watching or something?
                        currentMapper = null;
                        fullPath = null;
                        return storingState;
                    }

                    if (currentMapper instanceof ObjectMapper om) {
                        parents.push(new Parent(om, depth));
                        currentMapper = null;
                    }
                    depth += 1;
                }
                case Token.EndObject endObject -> {
                    assert depth > 0;

                    if (documents.peek().depth == depth) {
                        documents.pop();
                    }

                    depth -= 1;

                    if (parents.peek().depth() == depth) {
                        parents.pop();
                    }
                }
                case Token.StartArray startArray -> {
                    if (currentMapper instanceof ObjectMapper om) {
                        parents.push(new Parent(om, depth));
                        currentMapper = null;
                    }
                    depth += 1;
                }
                case Token.EndArray endArray -> {
                    assert depth > 0;
                    depth -= 1;

                    if (parents.peek().depth() == depth) {
                        parents.pop();
                    }
                }
                case Token.FieldName fieldName -> {
                    ObjectMapper parentMapper = parents.peek().parentMapper();
                    fullPath = parentMapper.isRoot() ? fieldName.name() : parentMapper.fullPath() + "." + fieldName.name();
                    currentMapper = parentMapper.getMapper(fieldName.name());
                }
                default -> {
                }
            }

            return this;
        }

        public State consume(Event event) {
            switch (event) {
                case Event.DocumentSwitch ds -> documents.push(new Document(ds.document(), depth));
            }

            return this;
        }

        record Parent(ObjectMapper parentMapper, int depth) {}

        record Document(LuceneDocument document, int depth) {}
    }
}
