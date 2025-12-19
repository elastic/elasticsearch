/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * A custom implementation of {@link org.apache.lucene.index.BinaryDocValues} that uses a {@link Set} to maintain a collection of unique
 * binary doc values for fields with multiple values per document.
 */
public abstract class MultiValuedBinaryDocValuesField extends CustomDocValuesField {
    public enum Ordering {
        INSERTION,
        NATURAL
    }

    protected final Set<BytesRef> uniqueValues;
    protected int docValuesByteCount = 0;

    MultiValuedBinaryDocValuesField(String name, Ordering ordering) {
        super(name);

        uniqueValues = switch (ordering) {
            case INSERTION -> new LinkedHashSet<>();
            case NATURAL -> new TreeSet<>();
        };
    }

    public void add(BytesRef value) {
        if (uniqueValues.add(value)) {
            // might as well track these on the go as opposed to having to loop through all entries later
            docValuesByteCount += value.length;
        }
    }

    public int count() {
        return uniqueValues.size();
    }

    protected void writeLenAndValues(BytesStreamOutput out) throws IOException {
        for (BytesRef value : uniqueValues) {
            int valueLength = value.length;
            out.writeVInt(valueLength);
            out.writeBytes(value.bytes, value.offset, valueLength);
        }
    }

    @Override
    public abstract BytesRef binaryValue();

    public static class IntegratedCount extends MultiValuedBinaryDocValuesField {
        IntegratedCount(String name, Ordering ordering) {
            super(name, ordering);
        }

        /**
         * Encodes the collection of BytesRef into a single BytesRef, using the form:
         * [doc value count][length of value 1][value 1][length of value 2][value 2]...
         */
        @Override
        public BytesRef binaryValue() {
            int docValuesCount = uniqueValues.size();
            // the + 1 is for the total doc values count, which is prefixed at the start of the array
            int streamSize = docValuesByteCount + (docValuesCount + 1) * (Integer.BYTES + 1);

            try (BytesStreamOutput out = new BytesStreamOutput(streamSize)) {
                out.writeVInt(docValuesCount);
                writeLenAndValues(out);
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }
        }
    }

    public static class SeparateCount extends MultiValuedBinaryDocValuesField {
        public static final String COUNT_FIELD_SUFFIX = ".counts";

        private SeparateCount(String name, Ordering ordering) {
            super(name, ordering);
        }

        public static SeparateCount naturalOrder(String name) {
            return new SeparateCount(name, Ordering.NATURAL);
        }

        /**
         * Encodes the collection of BytesRef into a single BytesRef. Unlike IntegratedCount, the number of values is not stored, and
         * if there is only a single value, the length is not stored.
         * For multiple values, the format is: [length of value 1][value 1][length of value 2][value 2]...
         * For a single value, the format is: [value 1]
         */
        @Override
        public BytesRef binaryValue() {
            int docValuesCount = uniqueValues.size();
            int streamSize = docValuesByteCount + docValuesCount * (Integer.BYTES + 1);

            try (BytesStreamOutput out = new BytesStreamOutput(streamSize)) {
                if (docValuesCount == 1) {
                    BytesRef value = uniqueValues.iterator().next();
                    out.writeBytes(value.bytes, value.offset, value.length);
                } else {
                    writeLenAndValues(out);
                }
                return out.bytes().toBytesRef();
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to get binary value", e);
            }
        }

        public String countFieldName() {
            return name() + COUNT_FIELD_SUFFIX;
        }
    }

}
