/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;

import java.io.Reader;

// used for binary, geo and range fields
public abstract class CustomDocValuesField implements IndexableField {

    public static final FieldType TYPE = new FieldType();
    static {
        TYPE.setDocValuesType(DocValuesType.BINARY);
        TYPE.setOmitNorms(true);
        TYPE.freeze();
    }

    private final String name;

    protected CustomDocValuesField(String name) {
        this.name = name;
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
        return null;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        return null;
    }

}
