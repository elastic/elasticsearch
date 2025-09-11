/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;

public class PatternedTextDocValues extends BinaryDocValues {
    private final SortedSetDocValues templateDocValues;
    private final SortedSetDocValues argsDocValues;
    private final SortedSetDocValues argsInfoDocValues;

    PatternedTextDocValues(SortedSetDocValues templateDocValues, SortedSetDocValues argsDocValues, SortedSetDocValues argsInfoDocValues) {
        this.templateDocValues = templateDocValues;
        this.argsDocValues = argsDocValues;
        this.argsInfoDocValues = argsInfoDocValues;
    }

    static PatternedTextDocValues from(LeafReader leafReader, String templateFieldName, String argsFieldName, String argsInfoFieldName)
        throws IOException {
        SortedSetDocValues templateDocValues = DocValues.getSortedSet(leafReader, templateFieldName);
        SortedSetDocValues argsDocValues = DocValues.getSortedSet(leafReader, argsFieldName);
        SortedSetDocValues argsInfoDocValues = DocValues.getSortedSet(leafReader, argsInfoFieldName);
        return new PatternedTextDocValues(templateDocValues, argsDocValues, argsInfoDocValues);
    }

    private String getNextStringValue() throws IOException {
        assert templateDocValues.docValueCount() == 1;
        String template = templateDocValues.lookupOrd(templateDocValues.nextOrd()).utf8ToString();
        List<Arg.Info> argsInfo = Arg.decodeInfo(argsInfoDocValues.lookupOrd(argsInfoDocValues.nextOrd()).utf8ToString());

        if (argsInfo.isEmpty() == false) {
            assert argsDocValues.docValueCount() == 1;
            assert argsInfoDocValues.docValueCount() == 1;
            var mergedArgs = argsDocValues.lookupOrd(argsDocValues.nextOrd());
            var args = Arg.decodeRemainingArgs(mergedArgs.utf8ToString());
            return PatternedTextValueProcessor.merge(template, args, argsInfo);
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
        return i == advance(i);
    }

    @Override
    public int docID() {
        return templateDocValues.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        advance(docID() + 1);
        return docID();
    }

    @Override
    public int advance(int i) throws IOException {
        int templateAdvance = templateDocValues.advance(i);
        var argsAdvance = argsDocValues.advance(templateAdvance);
        var argsInfoAdvance = argsInfoDocValues.advance(templateAdvance);
        assert argsAdvance >= templateAdvance;
        assert argsInfoAdvance == templateAdvance;
        return templateAdvance;
    }

    @Override
    public long cost() {
        return templateDocValues.cost() + argsDocValues.cost() + argsInfoDocValues.cost();
    }
}
