/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

/**
 * Block loader for fields that use fallback synthetic source implementation.
 * <br>
 * Usually fields have doc_values or stored fields and block loaders use them directly. In some cases neither is available
 * and we would fall back to (potentially synthetic) _source. However, in case of synthetic source, there is actually no need to
 * construct the entire _source. We know that there is no doc_values and stored fields, and therefore we will be using fallback synthetic
 * source. That is equivalent to just reading _ignored_source stored field directly and doing an in-place synthetic source just
 * for this field.
 * <br>
 * See {@link IgnoredSourceFieldMapper}.
 */
public abstract class FallbackSyntheticSourceBlockLoader implements BlockLoader {
    private final Reader<?> reader;
    private final String fieldName;

    protected FallbackSyntheticSourceBlockLoader(Reader<?> reader, String fieldName) {
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public ColumnAtATimeReader columnAtATimeReader(LeafReaderContext context) throws IOException {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(LeafReaderContext context) throws IOException {
        return new IgnoredSourceRowStrideReader<>(fieldName, reader);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return new StoredFieldsSpec(false, false, Set.of(IgnoredSourceFieldMapper.NAME));
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    private record IgnoredSourceRowStrideReader<T>(String fieldName, Reader<T> reader) implements RowStrideReader {
        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            var ignoredSource = storedFields.storedFields().get(IgnoredSourceFieldMapper.NAME);
            if (ignoredSource == null) {
                return;
            }

            Map<String, List<IgnoredSourceFieldMapper.NameValue>> valuesForFieldAndParents = new HashMap<>();

            // Contains name of the field and all its parents
            Set<String> fieldNames = new HashSet<>() {
                {
                    add("_doc");
                }
            };

            var current = new StringBuilder();
            for (String part : fieldName.split("\\.")) {
                if (current.isEmpty() == false) {
                    current.append('.');
                }
                current.append(part);
                fieldNames.add(current.toString());
            }

            for (Object value : ignoredSource) {
                IgnoredSourceFieldMapper.NameValue nameValue = IgnoredSourceFieldMapper.decode(value);
                if (fieldNames.contains(nameValue.name())) {
                    valuesForFieldAndParents.computeIfAbsent(nameValue.name(), k -> new ArrayList<>()).add(nameValue);
                }
            }

            // TODO figure out how to handle XContentDataHelper#voidValue()

            var blockValues = new ArrayList<T>();

            var leafFieldValue = valuesForFieldAndParents.get(fieldName);
            if (leafFieldValue != null) {
                readFromFieldValue(leafFieldValue, blockValues);
            } else {
                readFromParentValue(valuesForFieldAndParents, blockValues);
            }

            if (blockValues.isEmpty() == false) {
                if (blockValues.size() > 1) {
                    builder.beginPositionEntry();
                }

                reader.writeToBlock(blockValues, builder);

                if (blockValues.size() > 1) {
                    builder.endPositionEntry();
                }
            } else {
                builder.appendNull();
            }
        }

        private void readFromFieldValue(List<IgnoredSourceFieldMapper.NameValue> nameValues, List<T> blockValues) throws IOException {
            if (nameValues.isEmpty()) {
                return;
            }

            for (var nameValue : nameValues) {
                // Leaf field is stored directly (not as a part of a parent object), let's try to decode it.
                Optional<Object> singleValue = XContentDataHelper.decode(nameValue.value());
                if (singleValue.isPresent()) {
                    reader.convertValue(singleValue.get(), blockValues);
                    continue;
                }

                // We have a value for this field but it's an array or an object
                var type = XContentDataHelper.decodeType(nameValue.value());
                assert type.isPresent();

                try (
                    XContentParser parser = type.get()
                        .xContent()
                        .createParser(
                            XContentParserConfiguration.EMPTY,
                            nameValue.value().bytes,
                            nameValue.value().offset + 1,
                            nameValue.value().length - 1
                        )
                ) {
                    parser.nextToken();
                    parseWithReader(parser, blockValues);
                }
            }
        }

        private void readFromParentValue(
            Map<String, List<IgnoredSourceFieldMapper.NameValue>> valuesForFieldAndParents,
            List<T> blockValues
        ) throws IOException {
            if (valuesForFieldAndParents.isEmpty()) {
                return;
            }

            // If a parent object is stored at a particular level its children won't be stored.
            // So we should only ever have one parent here.
            assert valuesForFieldAndParents.size() == 1 : "_ignored_source field contains multiple levels of the same object";
            var parentValues = valuesForFieldAndParents.values().iterator().next();

            for (var nameValue : parentValues) {
                parseFieldFromParent(nameValue, blockValues);
            }
        }

        private void parseFieldFromParent(IgnoredSourceFieldMapper.NameValue nameValue, List<T> blockValues) throws IOException {
            var type = XContentDataHelper.decodeType(nameValue.value());
            assert type.isPresent();

            String nameAtThisLevel = fieldName.substring(nameValue.name().length() + 1);
            var filterParserConfig = XContentParserConfiguration.EMPTY.withFiltering(null, Set.of(nameAtThisLevel), Set.of(), true);
            try (
                XContentParser parser = type.get()
                    .xContent()
                    .createParser(filterParserConfig, nameValue.value().bytes, nameValue.value().offset + 1, nameValue.value().length - 1)
            ) {
                parser.nextToken();
                var fieldNames = new Stack<String>() {
                    {
                        push(nameValue.name());
                    }
                };

                while (parser.currentToken() != null) {
                    // We are descending into an object/array hierarchy of arbitrary depth
                    // until we find the field that we need.
                    while (true) {
                        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                            fieldNames.push(parser.currentName());
                            var nameInParser = String.join(".", fieldNames);
                            if (nameInParser.equals(fieldName)) {
                                parser.nextToken();
                                break;
                            }
                        } else {
                            assert parser.currentToken() == XContentParser.Token.START_OBJECT
                                || parser.currentToken() == XContentParser.Token.START_ARRAY;
                        }

                        parser.nextToken();
                    }
                    parseWithReader(parser, blockValues);
                    parser.nextToken();

                    // We are coming back up in object/array hierarchy.
                    // If arrays are present we will explore all array items by going back down again.
                    while (parser.currentToken() == XContentParser.Token.END_OBJECT
                        || parser.currentToken() == XContentParser.Token.END_ARRAY) {
                        // When exiting an object arrays we'll see END_OBJECT followed by END_ARRAY, but we only need to pop the object name
                        // once.
                        if (parser.currentToken() == XContentParser.Token.END_OBJECT) {
                            fieldNames.pop();
                        }
                        parser.nextToken();
                    }
                }
            }
        }

        private void parseWithReader(XContentParser parser, List<T> blockValues) throws IOException {
            reader.parse(parser, blockValues);
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return true;
        }
    }

    /**
     * Field-specific implementation that converts data stored in _ignored_source field to block loader values.
     * @param <T>
     */
    public interface Reader<T> {
        /**
         * Converts a raw stored value for this field to a value in a format suitable for block loader and adds it to the provided
         * accumulator.
         * @param value raw decoded value from _ignored_source field (synthetic _source value)
         * @param accumulator list containing the result of conversion
         */
        void convertValue(Object value, List<T> accumulator);

        /**
         * Parses one or more complex values using a provided parser and adds them to the provided accumulator.
         * @param parser parser of a value from _ignored_source field (synthetic _source value)
         * @param accumulator list containing the results of parsing
         */
        void parse(XContentParser parser, List<T> accumulator) throws IOException;

        void writeToBlock(List<T> values, Builder blockBuilder);
    }

    /**
     * Reader for field types that don't parse arrays (arrays are always treated as multiple values)
     * as opposed to field types that treat arrays as special cases (for example point).
     * @param <T>
     */
    public abstract static class SingleValueReader<T> implements Reader<T> {
        private final Object nullValue;

        public SingleValueReader(Object nullValue) {
            this.nullValue = nullValue;
        }

        @Override
        public void parse(XContentParser parser, List<T> accumulator) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                if (nullValue != null) {
                    convertValue(nullValue, accumulator);
                }
                return;
            }
            if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                        if (nullValue != null) {
                            convertValue(nullValue, accumulator);
                        }
                    } else {
                        parseNonNullValue(parser, accumulator);
                    }
                }
                return;
            }

            parseNonNullValue(parser, accumulator);
        }

        protected abstract void parseNonNullValue(XContentParser parser, List<T> accumulator) throws IOException;
    }
}
