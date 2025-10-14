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
 * If there are values which exceed 32kb, they cannot be stored as doc values
 * and must be in a stored field. This class combines the doc values with the
 * larges values which are in stored fields. Despite being backed by stored
 * fields, this class implements a doc value interface.
 */
public final class PatternTextCompositeValues extends BinaryDocValues {
    private final LeafStoredFieldLoader storedTemplateLoader;
    private final String storedMessageFieldName;
    private final BinaryDocValues patternTextDocValues;
    private final SortedSetDocValues templateIdDocValues;
    private boolean hasDocValue = false;

    PatternTextCompositeValues(
        LeafStoredFieldLoader storedTemplateLoader,
        String storedMessageFieldName,
        BinaryDocValues patternTextDocValues,
        SortedSetDocValues templateIdDocValues
    ) {
        this.storedTemplateLoader = storedTemplateLoader;
        this.storedMessageFieldName = storedMessageFieldName;
        this.patternTextDocValues = patternTextDocValues;
        this.templateIdDocValues = templateIdDocValues;
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
            fieldType.argsInfoFieldName()
        );
        StoredFieldLoader storedFieldLoader = StoredFieldLoader.create(false, Set.of(fieldType.storedNamed()));
        LeafStoredFieldLoader storedTemplateLoader = storedFieldLoader.getLoader(leafReader.getContext(), null);
        return new PatternTextCompositeValues(storedTemplateLoader, fieldType.storedNamed(), docValues, templateIdDocValues);
    }

    public BytesRef binaryValue() throws IOException {
        if (hasDocValue) {
            return patternTextDocValues.binaryValue();
        }

        // If there is no doc value, the value was too large and was put in a stored field
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
        if (hasValue && hasDocValue == false) {
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
        return templateIdDocValues.cost() + patternTextDocValues.cost();
    }
}
