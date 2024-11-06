/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class SyntheticSourceDocumentParserListener implements DocumentParserListener {
    private final MappingLookup mappingLookup;
    private final IndexSettings indexSettings;
    private final XContentType xContentType;

    private final List<IgnoredSourceFieldMapper.NameValue> valuesToStore;

    private State state;

    public SyntheticSourceDocumentParserListener(MappingLookup mappingLookup, IndexSettings indexSettings, XContentType xContentType) {
        this.mappingLookup = mappingLookup;
        this.indexSettings = indexSettings;
        this.xContentType = xContentType;

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

    @Override
    public Output finish() {
        return new Output(valuesToStore);
    }

    interface State {
        State consume(Token token) throws IOException;

        State consume(Event event);
    }

    class Storing implements State {
        private final State returnState;
        private final String fullPath;
        private final ObjectMapper parentMapper;
        private final LuceneDocument document;

        private final XContentBuilder data;

        private int depth;

        Storing(State returnState, Token startingToken, String fullPath, ObjectMapper parentMapper, LuceneDocument document)
            throws IOException {
            this.returnState = returnState;
            this.fullPath = fullPath;
            this.parentMapper = parentMapper;
            this.document = document;

            this.data = XContentBuilder.builder(xContentType.xContent());

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
                case Token.StringAsCharArrayValue stringAsCharArrayValue -> data.generator()
                    .writeString(stringAsCharArrayValue.buffer(), stringAsCharArrayValue.offset(), stringAsCharArrayValue.length());
                case Token.ValueToken<?> valueToken -> data.value(valueToken.value());
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

        private Mapper currentMapper;
        private int depth;

        Watching(RootObjectMapper rootMapper) {
            this.parents = new Stack<>() {
                {
                    push(new Watching.Parent(rootMapper, 0));
                }
            };
            this.documents = new Stack<>();

            this.currentMapper = rootMapper;
            this.depth = 0;
        }

        public State consume(Token token) throws IOException {
            switch (token) {
                case Token.StartObject startObject -> {
                    if (currentMapper instanceof ObjectMapper om && om.isEnabled() == false) {
                        ObjectMapper parentMapper = parents.peek().parentMapper();
                        String fullPath = parentMapper.isRoot()
                            ? currentMapper.leafName()
                            : parentMapper.fullPath() + "." + currentMapper.leafName();

                        prepare();

                        return new Storing(this, startObject, fullPath, parents.peek().parentMapper(), documents.peek().document());
                    }

                    if (currentMapper instanceof ObjectMapper om) {
                        parents.push(new Parent(om, depth));
                        prepare();
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
                    if (currentMapper instanceof ObjectMapper om
                        && (sourceKeepMode(om) == Mapper.SourceKeepMode.ALL
                            || (om.isNested() == false && sourceKeepMode(om) == Mapper.SourceKeepMode.ARRAYS))) {
                        ObjectMapper parentMapper = parents.peek().parentMapper();
                        String fullPath = parentMapper.isRoot()
                            ? currentMapper.leafName()
                            : parentMapper.fullPath() + "." + currentMapper.leafName();

                        prepare();

                        return new Storing(this, startArray, fullPath, parents.peek().parentMapper(), documents.peek().document());
                    }

                    if (currentMapper instanceof ObjectMapper om) {
                        parents.push(new Parent(om, depth));
                        prepare();
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
                    currentMapper = parentMapper.getMapper(fieldName.name());
                }
                default -> {
                }
            }

            return this;
        }

        /**
         * Resets the state to prepare for new part of the document (e.g. descend into an object).
         */
        private void prepare() {
            currentMapper = null;
        }

        private Mapper.SourceKeepMode sourceKeepMode(ObjectMapper mapper) {
            return mapper.sourceKeepMode().orElseGet(indexSettings::sourceKeepMode);
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
