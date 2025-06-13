/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PatternedTextDocValues extends BinaryDocValues {
    private final SortedSetDocValues templateDocValues;
    private final SortedSetDocValues argsDocValues;

    PatternedTextDocValues(SortedSetDocValues templateDocValues, SortedSetDocValues argsDocValues) {
        this.templateDocValues = templateDocValues;
        this.argsDocValues = argsDocValues;
    }

    static PatternedTextDocValues from(LeafReader leafReader, String templateFieldName, String argsFieldName) throws IOException {
        SortedSetDocValues templateDocValues = DocValues.getSortedSet(leafReader, templateFieldName);
        if (templateDocValues.getValueCount() == 0) {
            return null;
        }

        SortedSetDocValues argsDocValues = DocValues.getSortedSet(leafReader, argsFieldName);
        return new PatternedTextDocValues(templateDocValues, argsDocValues);
    }

    private String getNextStringValue() throws IOException {
        assert templateDocValues.docValueCount() == 1;
        String template = templateDocValues.lookupOrd(templateDocValues.nextOrd()).utf8ToString();
        int argsCount = PatternedTextValueProcessor.countArgs(template);
        if (argsCount > 0) {
            assert argsDocValues.docValueCount() == 1;
            var mergedArgs = argsDocValues.lookupOrd(argsDocValues.nextOrd());
            var args = PatternedTextValueProcessor.decodeRemainingArgs(mergedArgs.utf8ToString());
            return PatternedTextValueProcessor.merge(new PatternedTextValueProcessor.Parts(template, args));
        } else {
            return template;
        }
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return new BytesRef(getNextStringValue());
    }

    @Override
    public boolean advanceExact(int i) throws IOException {
        argsDocValues.advanceExact(i);
        // If template has a value, then message has a value. We don't have to check args here, since there may not be args for the doc
        return templateDocValues.advanceExact(i);
    }

    @Override
    public int docID() {
        return templateDocValues.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        int templateNext = templateDocValues.nextDoc();
        int argsNext = argsDocValues.nextDoc();
        assert templateNext == argsNext;
        return templateNext;
    }

    @Override
    public int advance(int i) throws IOException {
        int templateAdvance = templateDocValues.advance(i);
        int argAdvance = argsDocValues.advance(i);
        assert templateAdvance == argAdvance;
        return templateAdvance;
    }

    @Override
    public long cost() {
        return templateDocValues.cost() + argsDocValues.cost();
    }
}
