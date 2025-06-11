/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.patternedtext;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PatternedTextDocValues extends SortedSetDocValues {
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

    @Override
    public long nextOrd() throws IOException {
        return templateDocValues.nextOrd();
    }

    @Override
    public int docValueCount() {
        return templateDocValues.docValueCount();
    }

    @Override
    public BytesRef lookupOrd(long l) throws IOException {
        return new BytesRef(lookupOrdAsString(l));
    }

    String lookupOrdAsString(long l) throws IOException {
        String template = templateDocValues.lookupOrd(l).utf8ToString();
        int argsCount = PatternedTextValueProcessor.countArgs(template);
        List<String> args = new ArrayList<>(argsCount);
        if (argsCount > 0) {
            var mergedArgs = argsDocValues.lookupOrd(argsDocValues.nextOrd());
            PatternedTextValueProcessor.addRemainingArgs(args, mergedArgs.utf8ToString());
        }
        return PatternedTextValueProcessor.merge(new PatternedTextValueProcessor.Parts(template, args));
    }

    @Override
    public long getValueCount() {
        return templateDocValues.getValueCount();
    }

    @Override
    public boolean advanceExact(int i) throws IOException {
        argsDocValues.advanceExact(i);
        return templateDocValues.advanceExact(i);
    }

    @Override
    public int docID() {
        return templateDocValues.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return templateDocValues.nextDoc();
    }

    @Override
    public int advance(int i) throws IOException {
        return templateDocValues.advance(i);
    }

    @Override
    public long cost() {
        return templateDocValues.cost() + argsDocValues.cost();
    }
}
