/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.mapper.BinaryFieldMapper.CustomBinaryDocValuesField;

public class HighCardinalityKeywordFieldDataTests extends AbstractStringFieldDataTestCase {

    @Override
    protected String getFieldDataType() {
        return "keyword_high_cardinality";
    }

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    @Override
    protected long minRamBytesUsed() {
        return 0;
    }

    private Document current;
    private Map<String, CustomBinaryDocValuesField> fields = new HashMap<>();

    @Override
    protected void addField(Document d, String name, String value) {
        if (current != d) {
            current = d;
            fields.clear();
        }

        d.add(new StringField(name, value, Field.Store.YES));
        var field =  fields.get(name);
        if (field != null) {
            fields.get(name).add(value.getBytes(StandardCharsets.UTF_8));
        } else {
            field = new CustomBinaryDocValuesField(name, value.getBytes(StandardCharsets.UTF_8));
            fields.put(name, field);
            d.add(field);
        }
    }



    // Unsupported tests:
    public void testGlobalOrdinalsGetRemovedOnceIndexReaderCloses() throws Exception {
        assumeFalse("binary doc values don't support ordinals or global ordinals", true);
    }

    public void testSingleValuedGlobalOrdinals() throws Exception {
        assumeFalse("binary doc values don't support ordinals or global ordinals", true);
    }

    public void testGlobalOrdinals() throws Exception {
        assumeFalse("binary doc values don't support ordinals or global ordinals", true);
    }

    public void testTermsEnum() throws Exception {
        assumeFalse("binary doc values don't support terms enum", true);
    }
}
