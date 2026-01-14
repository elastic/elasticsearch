/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Values which exceeds 32kb cannot be stored as sorted set doc values. Such values must be stored separately in binary doc values, which
 * do not have length limitations. This class combines the regular doc values with the raw values from binary doc values.
 */
public final class PatternTextCompositeValues extends BinaryDocValues {

    private final LeafStoredFieldLoader storedTemplateLoader;
    private final String storedMessageFieldName;
    private final BinaryDocValues patternTextDocValues;
    private final SortedSetDocValues templateIdDocValues;
    private final BinaryDocValues rawTextDocValues;
    private boolean hasDocValue = false;
    private boolean hasRawTextDocValue = false;

    PatternTextCompositeValues(
        LeafStoredFieldLoader storedTemplateLoader,
        String storedMessageFieldName,
        BinaryDocValues patternTextDocValues,
        SortedSetDocValues templateIdDocValues,
        BinaryDocValues rawTextDocValues
    ) {
        this.storedTemplateLoader = storedTemplateLoader;
        this.storedMessageFieldName = storedMessageFieldName;
        this.patternTextDocValues = patternTextDocValues;
        this.templateIdDocValues = templateIdDocValues;
        this.rawTextDocValues = rawTextDocValues;
    }

    static PatternTextCompositeValues from(LeafReader leafReader, PatternTextFieldType fieldType) throws IOException {
        SortedSetDocValues templateIdDocValues = DocValues.getSortedSet(leafReader, fieldType.templateIdFieldName());
        if (templateIdDocValues.getValueCount() == 0) {
            return null;
        }

        var docValues = PatternTextDocValues.from(
            leafReader,
            fieldType.templateFieldName(),
            fieldType.argsFieldName(),
            fieldType.argsInfoFieldName(),
            fieldType.useBinaryDocValuesArgs()
        );

        // load binary doc values (for newer indices that store raw values in binary doc values)
        BinaryDocValues rawBinaryDocValues = leafReader.getBinaryDocValues(fieldType.storedNamed());
        if (rawBinaryDocValues == null) {
            // use an empty object here to avoid making null checks later
            rawBinaryDocValues = DocValues.emptyBinary();
        }

        // load stored field loader (for older indices that store raw values in stored fields)
        StoredFieldLoader storedFieldLoader = StoredFieldLoader.create(false, Set.of(fieldType.storedNamed()));
        LeafStoredFieldLoader storedTemplateLoader = storedFieldLoader.getLoader(leafReader.getContext(), null);

        return new PatternTextCompositeValues(
            storedTemplateLoader,
            fieldType.storedNamed(),
            docValues,
            templateIdDocValues,
            rawBinaryDocValues
        );
    }

    public BytesRef binaryValue() throws IOException {
        if (hasDocValue) {
            return patternTextDocValues.binaryValue();
        }

        // if there is no doc value, then the value was too large to be analyzed or templating was disabled

        // for newer indices, the value is stored in binary doc values
        if (hasRawTextDocValue) {
            return rawTextDocValues.binaryValue();
        }

        // for older indices, it's stored in stored fields
        var storedFields = storedTemplateLoader.storedFields();
        List<Object> storedValues = storedFields.get(storedMessageFieldName);
        assert storedValues != null && storedValues.size() == 1 && storedValues.getFirst() instanceof BytesRef;
        return (BytesRef) storedValues.getFirst();
    }

    public int docID() {
        return templateIdDocValues.docID();
    }

    public boolean advanceExact(int i) throws IOException {
        boolean hasValue = templateIdDocValues.advanceExact(i);
        hasDocValue = patternTextDocValues.advanceExact(i);
        hasRawTextDocValue = rawTextDocValues.advanceExact(i);
        if (hasValue && hasDocValue == false && hasRawTextDocValue == false) {
            storedTemplateLoader.advanceTo(i);
        }
        return hasValue;
    }

    @Override
    public int nextDoc() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int i) {
        throw new UnsupportedOperationException();

    }

    @Override
    public long cost() {
        return templateIdDocValues.cost() + patternTextDocValues.cost() + rawTextDocValues.cost();
    }
}
