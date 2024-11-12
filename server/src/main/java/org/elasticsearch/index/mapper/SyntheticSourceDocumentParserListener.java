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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

class SyntheticSourceDocumentParserListener implements DocumentParserListener {
    private final IndexSettings indexSettings;
    private final XContentType xContentType;

    private final Map<LuceneDocument, Map<String, List<StoredValue>>> ignoredSourceValues;

    private State state;

    SyntheticSourceDocumentParserListener(MappingLookup mappingLookup, IndexSettings indexSettings, XContentType xContentType) {
        this.indexSettings = indexSettings;
        this.xContentType = xContentType;

        this.ignoredSourceValues = new HashMap<>();
        this.state = new Tracking(mappingLookup.getMapping().getRoot());
    }

    @Override
    public boolean isActive() {
        return state instanceof Storing;
    }

    @Override
    public void consume(Token token) throws IOException {
        if (token == null) {
            return;
        }

        this.state = state.consume(token);
    }

    @Override
    public void consume(Event event) throws IOException {
        if (event == null) {
            return;
        }

        if (event instanceof Event.DocumentSwitch documentSwitch) {
            ignoredSourceValues.put(documentSwitch.document(), new HashMap<>());
        }

        this.state = state.consume(event);
    }

    @Override
    public Output finish() {
        var values = new ArrayList<IgnoredSourceFieldMapper.NameValue>();

        for (var fieldToValueMap : ignoredSourceValues.values()) {
            for (var fieldValues : fieldToValueMap.values()) {
                long singleElementArrays = 0;
                long stashedValuesForSourceKeepArrays = 0;

                for (var fieldValue : fieldValues) {
                    if (fieldValue instanceof StoredValue.Array arr) {
                        if (arr.length == 1 && arr.reason() == StoreReason.LEAF_STORED_ARRAY) {
                            singleElementArrays += 1;
                        }
                    }
                    if (fieldValue instanceof StoredValue.Singleton singleton) {
                        if (singleton.reason() == StoreReason.LEAF_VALUE_STASH_FOR_STORED_ARRAYS) {
                            stashedValuesForSourceKeepArrays += 1;
                        }
                    }
                }

                // Only if all values match one of the optimization criteria we skip them, otherwise add all of them to resulting list.
                if (singleElementArrays != fieldValues.size() && stashedValuesForSourceKeepArrays != fieldValues.size()) {
                    for (var storedValue : fieldValues) {
                        values.add(storedValue.nameValue());
                    }
                }
            }
        }

        return new Output(values);
    }

    sealed interface StoredValue permits StoredValue.Array, StoredValue.Singleton {
        IgnoredSourceFieldMapper.NameValue nameValue();

        /**
         * An array of values is stored f.e. due to synthetic_source_keep: "arrays".
         */
        record Array(IgnoredSourceFieldMapper.NameValue nameValue, StoreReason reason, long length) implements StoredValue {}

        /**
         * A single value.
         */
        record Singleton(IgnoredSourceFieldMapper.NameValue nameValue, StoreReason reason) implements StoredValue {}

    }

    /**
     * Reason for storing this value.
     */
    enum StoreReason {
        /**
         * Leaf array that is stored due to "synthetic_source_keep": "arrays".
         */
        LEAF_STORED_ARRAY,

        /**
         * "Stashed" value needed to only in case there are mixed arrays and single values
         * for this field.
         * Can be dropped in some cases.
         */
        LEAF_VALUE_STASH_FOR_STORED_ARRAYS,

        /**
         * There is currently no need to distinguish other reasons.
         */
        OTHER
    }

    private void addIgnoredSourceValue(StoredValue storedValue, String fullPath, LuceneDocument luceneDocument) {
        var values = ignoredSourceValues.get(luceneDocument).computeIfAbsent(fullPath, p -> new ArrayList<>());

        values.add(storedValue);
    }

    interface State {
        State consume(Token token) throws IOException;

        State consume(Event event) throws IOException;
    }

    class Storing implements State {
        private final State returnState;
        private final String fullPath;
        private final ObjectMapper parentMapper;
        private final StoreReason reason;
        private final LuceneDocument document;

        private final XContentBuilder data;
        private int depth;
        private int length;

        Storing(
            State returnState,
            Token startingToken,
            String fullPath,
            ObjectMapper parentMapper,
            StoreReason reason,
            LuceneDocument document
        ) throws IOException {
            this.returnState = returnState;
            this.fullPath = fullPath;
            this.parentMapper = parentMapper;
            this.reason = reason;
            this.document = document;

            this.data = XContentBuilder.builder(xContentType.xContent());

            this.depth = 0;
            this.length = 0;

            consume(startingToken);
        }

        public State consume(Token token) throws IOException {
            switch (token) {
                case Token.StartObject startObject -> {
                    data.startObject();
                    if (depth == 1) {
                        length += 1;
                    }
                    depth += 1;
                }
                case Token.EndObject endObject -> {
                    data.endObject();

                    if (processEndObjectOrArray(endObject)) {
                        return returnState;
                    }
                }
                case Token.StartArray startArray -> {
                    data.startArray();
                    depth += 1;
                }
                case Token.EndArray endArray -> {
                    data.endArray();

                    if (processEndObjectOrArray(endArray)) {
                        return returnState;
                    }
                }
                case Token.FieldName fieldName -> data.field(fieldName.name());
                case Token.StringAsCharArrayValue stringAsCharArrayValue -> {
                    if (depth == 1) {
                        length += 1;
                    }
                    data.generator()
                        .writeString(stringAsCharArrayValue.buffer(), stringAsCharArrayValue.offset(), stringAsCharArrayValue.length());
                }
                case Token.ValueToken<?> valueToken -> {
                    if (depth == 1) {
                        length += 1;
                    }
                    data.value(valueToken.value());
                }
                case Token.NullValue nullValue -> {
                    if (depth == 1) {
                        length += 1;
                    }
                    data.nullValue();
                }
                case null -> {
                }
            }

            return this;
        }

        public State consume(Event event) {
            // When nested objects are store we will receive `Event.DocumentSwitch` here.
            // However, ignored source value needs to be stored in the parent document to take precedence
            // properly.
            // So it's ignored.
            // Other event types are not relevant to storage logic as well (at the time of writing).

            return this;
        }

        private boolean processEndObjectOrArray(Token token) throws IOException {
            assert token instanceof Token.EndObject || token instanceof Token.EndArray
                : "Unexpected token when storing ignored source value";

            depth -= 1;
            if (depth == 0) {
                var parentOffset = parentMapper.isRoot() ? 0 : parentMapper.fullPath().length() + 1;
                var nameValue = new IgnoredSourceFieldMapper.NameValue(
                    fullPath,
                    parentOffset,
                    XContentDataHelper.encodeXContentBuilder(data),
                    document
                );
                var storedValue = token instanceof Token.EndObject
                    ? new StoredValue.Singleton(nameValue, reason)
                    : new StoredValue.Array(nameValue, reason, length);

                addIgnoredSourceValue(storedValue, fullPath, document);

                return true;
            }

            return false;
        }
    }

    class Tracking implements State {
        private final Deque<Parent> parents;
        private final Stack<Document> documents;

        private int depth;

        Tracking(RootObjectMapper rootMapper) {
            this.parents = new ArrayDeque<>() {
                {
                    push(new Tracking.Parent(rootMapper, Parent.Type.OBJECT, 0));
                }
            };
            this.documents = new Stack<>();
            this.depth = 0;
        }

        public State consume(Token token) throws IOException {
            return this;
        }

        public State consume(Event event) throws IOException {
            switch (event) {
                case Event.DocumentSwitch documentSwitch -> documents.push(new Document(documentSwitch.document(), depth));
                case Event.DocumentStart documentStart -> {
                    if (documentStart.rootObjectMapper().isEnabled() == false) {
                        return new Storing(
                            this,
                            Token.START_OBJECT,
                            documentStart.rootObjectMapper().fullPath(),
                            documentStart.rootObjectMapper(),
                            StoreReason.OTHER,
                            documents.peek().document()
                        );
                    }
                }
                case Event.ObjectStart objectStart -> {
                    depth += 1;

                    var storeReason = shouldStoreObject(objectStart);
                    if (storeReason != null) {
                        return new Storing(
                            this,
                            Token.START_OBJECT,
                            objectStart.objectMapper().fullPath(),
                            parents.peek().parentMapper(),
                            storeReason,
                            documents.peek().document()
                        );
                    }

                    parents.push(new Parent(objectStart.objectMapper(), Parent.Type.OBJECT, depth));
                }
                case Event.ObjectEnd objectEnd -> {
                    assert depth > 0;

                    if (documents.peek().depth == depth) {
                        documents.pop();
                    }

                    if (parents.peek().depth() == depth) {
                        parents.pop();
                    }

                    depth -= 1;
                }
                case Event.ObjectArrayStart objectArrayStart -> {
                    depth += 1;

                    var storeReason = shouldStoreObjectArray(objectArrayStart);
                    if (storeReason != null) {
                        return new Storing(
                            this,
                            Token.START_ARRAY,
                            objectArrayStart.objectMapper().fullPath(),
                            parents.peek().parentMapper(),
                            storeReason,
                            documents.peek().document()
                        );
                    }

                    parents.push(new Parent(objectArrayStart.objectMapper(), Parent.Type.ARRAY, depth));
                }
                case Event.ObjectArrayEnd objectArrayEnd -> {
                    assert depth > 0;

                    if (parents.peek().depth() == depth) {
                        parents.pop();
                    }

                    depth -= 1;
                }
                case Event.LeafValue leafValue -> {
                    var storeReason = shouldStoreLeaf(leafValue);
                    if (storeReason != null) {
                        if (leafValue.isComplexValue()) {
                            return new Storing(
                                this,
                                leafValue.isArray() ? Token.START_ARRAY : Token.START_OBJECT,
                                leafValue.fieldMapper().fullPath(),
                                parents.peek().parentMapper(),
                                storeReason,
                                documents.peek().document()
                            );
                        }

                        var parentMapper = parents.peek().parentMapper();
                        var parentOffset = parentMapper.isRoot() ? 0 : parentMapper.fullPath().length() + 1;

                        var nameValue = new IgnoredSourceFieldMapper.NameValue(
                            leafValue.fieldMapper().fullPath(),
                            parentOffset,
                            leafValue.encodeValue(),
                            documents.peek().document()
                        );
                        addIgnoredSourceValue(
                            new StoredValue.Singleton(nameValue, storeReason),
                            leafValue.fieldMapper().fullPath(),
                            documents.peek().document()
                        );
                    }
                }
                case Event.LeafArrayStart leafArrayStart -> {
                    var storeReason = shouldStoreLeafArray(leafArrayStart);
                    if (storeReason != null) {
                        // TODO generalize this
                        return new Storing(
                            this,
                            Token.START_ARRAY,
                            leafArrayStart.fieldMapper().fullPath(),
                            parents.peek().parentMapper(),
                            storeReason,
                            documents.peek().document()
                        );
                    }
                }
                case Event.LeafArrayEnd leafArrayEnd -> {
                }
            }

            return this;
        }

        private StoreReason shouldStoreObject(Event.ObjectStart objectStart) {
            var sourceKeepMode = sourceKeepMode(objectStart.objectMapper());

            // This is a special case of "stashing" a single value in case this field
            // has "mixed" values inside an object array - some values are arrays and some are singletons.
            // Since ignored source always takes precedence over standard synthetic source logic
            // in such a case we would display only values from arrays and lose singleton values.
            // This additional logic fixes it by storing single values in ignored source as well.
            if (sourceKeepMode == Mapper.SourceKeepMode.ARRAYS && insideHigherLevelObjectArray()) {
                return StoreReason.OTHER;
            }

            if (objectStart.objectMapper().isEnabled() == false || sourceKeepMode == Mapper.SourceKeepMode.ALL) {
                return SyntheticSourceDocumentParserListener.StoreReason.OTHER;
            }

            return null;
        }

        private StoreReason shouldStoreObjectArray(Event.ObjectArrayStart objectArrayStart) {
            var sourceKeepMode = sourceKeepMode(objectArrayStart.objectMapper());
            if (sourceKeepMode == Mapper.SourceKeepMode.ARRAYS && objectArrayStart.objectMapper().isNested() == false) {
                return StoreReason.OTHER;
            }

            if (sourceKeepMode == Mapper.SourceKeepMode.ALL) {
                return StoreReason.OTHER;
            }

            return null;
        }

        private StoreReason shouldStoreLeaf(Event.LeafValue leafValue) {
            var sourceKeepMode = sourceKeepMode(leafValue.fieldMapper());
            if (sourceKeepMode == Mapper.SourceKeepMode.ARRAYS && insideHigherLevelObjectArray()) {
                return StoreReason.LEAF_VALUE_STASH_FOR_STORED_ARRAYS;
            }
            if (sourceKeepMode == Mapper.SourceKeepMode.ALL) {
                return StoreReason.OTHER;
            }
            if (leafValue.fieldMapper().syntheticSourceMode() == FieldMapper.SyntheticSourceMode.FALLBACK) {
                return StoreReason.OTHER;
            }

            return null;
        }

        private StoreReason shouldStoreLeafArray(Event.LeafArrayStart leafArrayStart) {
            var sourceKeepMode = sourceKeepMode(leafArrayStart.fieldMapper());
            if (sourceKeepMode == Mapper.SourceKeepMode.ARRAYS) {
                return StoreReason.LEAF_STORED_ARRAY;
            }
            if (sourceKeepMode == Mapper.SourceKeepMode.ALL) {
                return StoreReason.OTHER;
            }

            return null;
        }

        private Mapper.SourceKeepMode sourceKeepMode(ObjectMapper mapper) {
            return mapper.sourceKeepMode().orElseGet(indexSettings::sourceKeepMode);
        }

        private Mapper.SourceKeepMode sourceKeepMode(FieldMapper mapper) {
            return mapper.sourceKeepMode().orElseGet(indexSettings::sourceKeepMode);
        }

        private boolean insideHigherLevelObjectArray() {
            for (var parent : parents) {
                if (parent.type() == Parent.Type.ARRAY) {
                    return true;
                }
            }

            return false;
        }

        record Parent(ObjectMapper parentMapper, Type type, int depth) {
            enum Type {
                OBJECT,
                ARRAY
            }
        }

        record Document(LuceneDocument document, int depth) {}
    }
}
