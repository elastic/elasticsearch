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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SyntheticSourceDocumentParserListener implements DocumentParserListener {
    private final CustomSyntheticSourceFieldLookup customSyntheticSourceFieldLookup;
    private final IndexSettings indexSettings;
    private final XContentType xContentType;

    private final Map<LuceneDocument, Map<String, List<StoredValue>>> ignoredSourceValues;

    private State state;

    SyntheticSourceDocumentParserListener(MappingLookup mappingLookup, IndexSettings indexSettings, XContentType xContentType) {
        this.customSyntheticSourceFieldLookup = mappingLookup.getCustomSyntheticSourceFieldLookup();
        this.indexSettings = indexSettings;
        this.xContentType = xContentType;

        this.ignoredSourceValues = new HashMap<>();
        this.state = new Tracking();
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

        this.state = state.consume(event);
    }

    private boolean shouldSkipEvent(Event event) {
        return switch (event) {
            case Event.LeafValue leafValue -> {
                var reason = customSyntheticSourceFieldLookup.getFieldsWithCustomSyntheticSourceHandling()
                    .get(leafValue.fieldMapper().fullPath());

                if (reason == null) {
                    yield true;
                }

                if (reason == CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS && leafValue.insideObjectArray() == false) {
                    yield true;
                }

                yield false;
            }
            case Event.ObjectStart objectStart -> {
                var reason = customSyntheticSourceFieldLookup.getFieldsWithCustomSyntheticSourceHandling()
                    .get(objectStart.objectMapper().fullPath());
                if (reason == null) {
                    yield true;
                }

                if (reason == CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS && objectStart.insideObjectArray() == false) {
                    yield true;
                }

                yield false;
            }
            default -> false;
        };
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
        var values = ignoredSourceValues.computeIfAbsent(luceneDocument, ld -> new HashMap<>())
            .computeIfAbsent(fullPath, p -> new ArrayList<>());

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
        public State consume(Token token) throws IOException {
            return this;
        }

        public State consume(Event event) throws IOException {
            switch (event) {
                case Event.DocumentStart documentStart -> {
                    if (documentStart.rootObjectMapper().isEnabled() == false) {
                        return new Storing(
                            this,
                            Token.START_OBJECT,
                            documentStart.rootObjectMapper().fullPath(),
                            documentStart.rootObjectMapper(),
                            StoreReason.OTHER,
                            documentStart.document()
                        );
                    }
                }
                case Event.ObjectStart objectStart -> {
                    var reason = customSyntheticSourceFieldLookup.getFieldsWithCustomSyntheticSourceHandling()
                        .get(objectStart.objectMapper().fullPath());
                    if (reason == null) {
                        return this;
                    }
                    if (reason == CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS && objectStart.insideObjectArray() == false) {
                        return this;
                    }

                    return new Storing(
                        this,
                        Token.START_OBJECT,
                        objectStart.objectMapper().fullPath(),
                        objectStart.parentMapper(),
                        StoreReason.OTHER,
                        objectStart.document()
                    );
                }
                case Event.ObjectEnd objectEnd -> {
                }
                case Event.ObjectArrayStart objectArrayStart -> {
                    var reason = customSyntheticSourceFieldLookup.getFieldsWithCustomSyntheticSourceHandling()
                        .get(objectArrayStart.objectMapper().fullPath());
                    if (reason == null) {
                        return this;
                    }

                    return new Storing(
                        this,
                        Token.START_ARRAY,
                        objectArrayStart.objectMapper().fullPath(),
                        objectArrayStart.parentMapper(),
                        StoreReason.OTHER,
                        objectArrayStart.document()
                    );
                }
                case Event.ObjectArrayEnd objectArrayEnd -> {
                }
                case Event.LeafValue leafValue -> {
                    var reason = customSyntheticSourceFieldLookup.getFieldsWithCustomSyntheticSourceHandling()
                        .get(leafValue.fieldMapper().fullPath());
                    if (reason == null) {
                        return this;
                    }
                    if (reason == CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS && leafValue.insideObjectArray() == false) {
                        return this;
                    }

                    var storeReason = reason == CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS
                        ? StoreReason.LEAF_VALUE_STASH_FOR_STORED_ARRAYS
                        : StoreReason.OTHER;

                    if (leafValue.isComplexValue()) {
                        return new Storing(
                            this,
                            leafValue.isArray() ? Token.START_ARRAY : Token.START_OBJECT,
                            leafValue.fieldMapper().fullPath(),
                            leafValue.parentMapper(),
                            storeReason,
                            leafValue.document()
                        );
                    }

                    var parentMapper = leafValue.parentMapper();
                    var parentOffset = parentMapper.isRoot() ? 0 : parentMapper.fullPath().length() + 1;

                    var nameValue = new IgnoredSourceFieldMapper.NameValue(
                        leafValue.fieldMapper().fullPath(),
                        parentOffset,
                        leafValue.encodeValue(),
                        leafValue.document()
                    );
                    addIgnoredSourceValue(
                        new StoredValue.Singleton(nameValue, storeReason),
                        leafValue.fieldMapper().fullPath(),
                        leafValue.document()
                    );
                }
                case Event.LeafArrayStart leafArrayStart -> {
                    var reason = customSyntheticSourceFieldLookup.getFieldsWithCustomSyntheticSourceHandling()
                        .get(leafArrayStart.fieldMapper().fullPath());
                    if (reason == null) {
                        return this;
                    }

                    var storeReason = reason == CustomSyntheticSourceFieldLookup.Reason.SOURCE_KEEP_ARRAYS
                        ? StoreReason.LEAF_STORED_ARRAY
                        : StoreReason.OTHER;
                    // TODO generalize this
                    return new Storing(
                        this,
                        Token.START_ARRAY,
                        leafArrayStart.fieldMapper().fullPath(),
                        leafArrayStart.parentMapper(),
                        storeReason,
                        leafArrayStart.document()
                    );
                }
                case Event.LeafArrayEnd leafArrayEnd -> {
                }
            }

            return this;
        }
    }
}
