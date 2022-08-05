/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Arrays;

/**
 * A utility class for fields that need to support autocomplete via
 * {@link MappedFieldType#getTerms(boolean, String, org.elasticsearch.index.query.SearchExecutionContext, String)}
 * but can't return a raw Lucene TermsEnum.
 */
public class SimpleTermCountEnum extends TermsEnum {
    int index = -1;
    String[] sortedTerms;
    String current = null;

    public SimpleTermCountEnum(String[] terms) {
        sortedTerms = Arrays.copyOf(terms, terms.length);
        Arrays.sort(sortedTerms);
    }

    public SimpleTermCountEnum(String termCount) {
        sortedTerms = new String[1];
        sortedTerms[0] = termCount;
    }

    @Override
    public BytesRef term() throws IOException {
        if (current == null) {
            return null;
        }
        return new BytesRef(current);
    }

    @Override
    public BytesRef next() throws IOException {
        index++;
        if (index >= sortedTerms.length) {
            current = null;
        } else {
            current = sortedTerms[index];
        }
        return term();
    }

    @Override
    public int docFreq() throws IOException {
        throw new UnsupportedOperationException();
    }

    // =============== All other TermsEnum methods not supported =================

    @Override
    public AttributeSource attributes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean seekExact(BytesRef text) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TermState termState() throws IOException {
        throw new UnsupportedOperationException();
    }

}
