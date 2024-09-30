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
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

public class SortedNumericFieldDataTests extends AbstractFieldDataImplTestCase {
    private void addField(Document d, String name, String value) {
        d.add(new StringField(name, value, Store.YES));
        d.add(new SortedSetDocValuesField(name, new BytesRef(value)));
    }

    private void addField(Document d, String name, Long value) {
        d.add(new LongField(name, value, Store.NO));
    }

    @Override
    protected void fillSingleValueAllSet() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", 2L);
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", 1L);
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", 3L);
        writer.addDocument(d);
    }

    @Override
    protected void add2SingleValuedDocumentsAndDeleteOneOfThem() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", 2L);
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        addField(d, "value", 4L);
        writer.addDocument(d);

        writer.commit();

        writer.deleteDocuments(new Term("_id", "1"));
    }

    @Override
    protected void fillSingleValueWithMissing() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", 2L);
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        // d.add(new StringField("value", one(), Field.Store.NO)); // MISSING....
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", 3L);
        writer.addDocument(d);
    }

    @Override
    protected void fillMultiValueAllSet() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", 2L);
        addField(d, "value", 4L);
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        addField(d, "value", 1L);
        writer.addDocument(d);
        writer.commit(); // TODO: Have tests with more docs for sorting

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", 3L);
        writer.addDocument(d);
    }

    @Override
    protected void fillMultiValueWithMissing() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        addField(d, "value", 2L);
        addField(d, "value", 4L);
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        // d.add(new StringField("value", one(), Field.Store.NO)); // MISSING
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        addField(d, "value", 3L);
        writer.addDocument(d);
    }

    @Override
    protected void fillAllMissing() throws Exception {
        Document d = new Document();
        addField(d, "_id", "1");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "2");
        writer.addDocument(d);

        d = new Document();
        addField(d, "_id", "3");
        writer.addDocument(d);
    }

    @Override
    protected void fillExtendedMvSet() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getFieldDataType() {
        return "long";
    }

    protected boolean hasDocValues() {
        return true;
    }

    protected long minRamBytesUsed() {
        // minimum number of bytes that this fielddata instance is expected to require
        return 0L;
    }

    public void testSortMultiValuesFields() {
        assumeTrue("Does not apply for Numeric doc values", false);
    }
}
