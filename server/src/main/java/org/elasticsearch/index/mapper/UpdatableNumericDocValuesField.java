/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;

import java.io.Reader;

public class UpdatableNumericDocValuesField implements IndexableField {

    public static final FieldType TYPE;

    static {
        FieldType ft = new FieldType();
        ft.setDocValuesType(DocValuesType.NUMERIC);
        ft.setOmitNorms(true);
        TYPE = Mapper.freezeAndDeduplicateFieldType(ft);
    }

    private final String name;
    private long value = 0;

    public UpdatableNumericDocValuesField(String name) {
        this.name = name;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public IndexableFieldType fieldType() {
        return TYPE;
    }

    @Override
    public String stringValue() {
        return null;
    }

    @Override
    public Reader readerValue() {
        return null;
    }

    @Override
    public Number numericValue() {
        return value;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        return null;
    }

    @Override
    public BytesRef binaryValue() {
        return null;
    }

    @Override
    public StoredValue storedValue() {
        return null;
    }

    @Override
    public InvertableType invertableType() {
        return InvertableType.BINARY;
    }
}
