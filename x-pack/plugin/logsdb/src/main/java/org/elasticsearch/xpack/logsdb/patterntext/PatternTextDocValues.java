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

import java.io.IOException;
import java.util.List;

public final class PatternTextDocValues extends BinaryDocValues {
    private final SortedSetDocValues templateDocValues;
    private final BinaryDocValues argsDocValues;
    private final SortedSetDocValues argsInfoDocValues;

    PatternTextDocValues(SortedSetDocValues templateDocValues, BinaryDocValues argsDocValues, SortedSetDocValues argsInfoDocValues) {
        this.templateDocValues = templateDocValues;
        this.argsDocValues = argsDocValues;
        this.argsInfoDocValues = argsInfoDocValues;
    }

    /**
     * Args columns was originally a SortedSetDocValues column and was replaced with BinaryDocValues.
     * To maintain backwards compatibility, if a BinaryDocValues column does not exist, use the old SortedSetDocValues.
     * Since pattern_text fields are not multivalued we can wrap the SortedSetDocValues in a BinaryDocValues interface.
     */
    static BinaryDocValues getArgsDocValues(LeafReader leafReader, String argsFieldName, boolean useBinaryDocValueArgs) throws IOException {
        if (useBinaryDocValueArgs) {
            return DocValues.getBinary(leafReader, argsFieldName);
        } else {
            var sortedSetDocValues = DocValues.getSortedSet(leafReader, argsFieldName);
            assert sortedSetDocValues != null;
            return new BinaryDelegatingSingletonSortedSetDocValues(sortedSetDocValues);
        }
    }

    private String getNextStringValue() throws IOException {
        assert templateDocValues.docValueCount() == 1;
        assert argsInfoDocValues.docValueCount() == 1;

        String template = templateDocValues.lookupOrd(templateDocValues.nextOrd()).utf8ToString();
        List<Arg.Info> argsInfo = Arg.decodeInfo(argsInfoDocValues.lookupOrd(argsInfoDocValues.nextOrd()).utf8ToString());
        if (argsInfo.isEmpty() == false) {
            var mergedArgs = argsDocValues.binaryValue();
            var args = Arg.decodeRemainingArgs(mergedArgs.utf8ToString());
            assert args.length == argsInfo.size();
            return PatternTextValueProcessor.merge(template, args, argsInfo);
        } else {
            return template;
        }
    }

    @Override
    public BytesRef binaryValue() throws IOException {
        return new BytesRef(getNextStringValue());
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
        if (templateDocValues.advanceExact(target)) {
            // If template has a value, then message has a value. We don't have to check args here, since there may not be args for the doc
            argsDocValues.advanceExact(target);
            argsInfoDocValues.advanceExact(target);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int docID() {
        return templateDocValues.docID();
    }

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
        return templateDocValues.cost() + argsDocValues.cost() + argsInfoDocValues.cost();
    }
}
