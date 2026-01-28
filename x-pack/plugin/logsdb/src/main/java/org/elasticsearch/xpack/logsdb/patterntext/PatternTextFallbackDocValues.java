/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.logsdb.patterntext.PatternTextDocValues.getArgsDocValues;

/**
 * Values which exceed 32kb cannot be stored as sorted set doc values. Such values must be stored outside sorted set doc values.
 * This class relies on {@link PatternTextDocValues} but can fall back to values that were stored seperately because limit was exceded.
 * <p>
 * Depending on index version the raw values may be stored in binary doc values or stored fields.
 */
public abstract class PatternTextFallbackDocValues extends BinaryDocValues {

    final BinaryDocValues patternTextDocValues;
    final SortedSetDocValues templateIdDocValues;
    boolean usePatternTextDocValues = false;

    PatternTextFallbackDocValues(BinaryDocValues patternTextDocValues, SortedSetDocValues templateIdDocValues) {
        this.patternTextDocValues = patternTextDocValues;
        this.templateIdDocValues = templateIdDocValues;
    }

    public int docID() {
        return templateIdDocValues.docID();
    }

    // TODO: add nextDoc(...) and advance(...) implementations that delegate to templateIdDocValues for completeness.
    // And add implementation for intoBitSet(...) / docIDRunEnd(...) that delegates to templateIdDocValues, which could speedup queries.
    @Override
    public int nextDoc() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        return templateIdDocValues.cost() + patternTextDocValues.cost();
    }

    static final class BinaryFallback extends PatternTextFallbackDocValues {

        final BinaryDocValues rawTextDocValues;

        BinaryFallback(BinaryDocValues patternTextDocValues, SortedSetDocValues templateIdDocValues, BinaryDocValues rawTextDocValues) {
            super(patternTextDocValues, templateIdDocValues);
            this.rawTextDocValues = rawTextDocValues;
        }

        public BytesRef binaryValue() throws IOException {
            if (usePatternTextDocValues) {
                return patternTextDocValues.binaryValue();
            }

            return rawTextDocValues.binaryValue();
        }

        public boolean advanceExact(int target) throws IOException {
            boolean hasValue = templateIdDocValues.advanceExact(target);
            usePatternTextDocValues = patternTextDocValues.advanceExact(target);
            if (hasValue && usePatternTextDocValues == false) {
                boolean advanced = rawTextDocValues.advanceExact(target);
                assert advanced;
            }
            return hasValue;
        }

        @Override
        public long cost() {
            return super.cost() + rawTextDocValues.cost();
        }
    }

    static final class LegacyStoredFieldFallback extends PatternTextFallbackDocValues {

        final String storedMessageFieldName;
        final LeafStoredFieldLoader storedTemplateLoader;

        LegacyStoredFieldFallback(
            LeafStoredFieldLoader storedTemplateLoader,
            String storedMessageFieldName,
            BinaryDocValues patternTextDocValues,
            SortedSetDocValues templateIdDocValues
        ) {
            super(patternTextDocValues, templateIdDocValues);
            this.storedTemplateLoader = storedTemplateLoader;
            this.storedMessageFieldName = storedMessageFieldName;
        }

        public BytesRef binaryValue() throws IOException {
            if (usePatternTextDocValues) {
                return patternTextDocValues.binaryValue();
            }
            // for older indices, it's stored in stored fields
            var storedFields = storedTemplateLoader.storedFields();
            List<Object> storedValues = storedFields.get(storedMessageFieldName);
            assert storedValues != null && storedValues.size() == 1 && storedValues.getFirst() instanceof BytesRef;
            return (BytesRef) storedValues.getFirst();
        }

        public boolean advanceExact(int target) throws IOException {
            boolean hasValue = templateIdDocValues.advanceExact(target);
            usePatternTextDocValues = patternTextDocValues.advanceExact(target);
            if (hasValue && usePatternTextDocValues == false) {
                storedTemplateLoader.advanceTo(target);
            }
            return hasValue;
        }
    }

    static BinaryDocValues from(LeafReader leafReader, PatternTextFieldType fieldType) throws IOException {
        SortedSetDocValues templateIdDocValues = DocValues.getSortedSet(leafReader, fieldType.templateIdFieldName());
        if (templateIdDocValues.getValueCount() == 0) {
            return null;
        }

        SortedSetDocValues templateDocValues = DocValues.getSortedSet(leafReader, fieldType.templateFieldName());
        BinaryDocValues argsDocValues = getArgsDocValues(leafReader, fieldType.argsFieldName(), fieldType.useBinaryDocValuesArgs());
        SortedSetDocValues argsInfoDocValues = DocValues.getSortedSet(leafReader, fieldType.argsInfoFieldName());
        var docValues = new PatternTextDocValues(templateDocValues, argsDocValues, argsInfoDocValues);

        FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(fieldType.storedNamed());
        if (fieldInfo == null) {
            // If there is no stored subfield (either binary doc values or stored field),
            // then there is no need to use PatternTextFallbackDocValues
            return docValues;
        }

        // load binary doc values (for newer indices that store raw values in binary doc values)
        if (fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
            BinaryDocValues rawBinaryDocValues = leafReader.getBinaryDocValues(fieldType.storedNamed());
            return new BinaryFallback(docValues, templateIdDocValues, rawBinaryDocValues);
        } else {
            // load stored field loader (for older indices that store raw values in stored fields)
            StoredFieldLoader storedFieldLoader = StoredFieldLoader.create(false, Set.of(fieldType.storedNamed()));
            LeafStoredFieldLoader storedTemplateLoader = storedFieldLoader.getLoader(leafReader.getContext(), null);
            return new LegacyStoredFieldFallback(storedTemplateLoader, fieldType.storedNamed(), docValues, templateIdDocValues);
        }
    }
}
