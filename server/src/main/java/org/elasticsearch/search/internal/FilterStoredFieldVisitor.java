/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;

public class FilterStoredFieldVisitor extends StoredFieldVisitor {
    private final StoredFieldVisitor visitor;

    public FilterStoredFieldVisitor(StoredFieldVisitor visitor) {
        this.visitor = visitor;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        visitor.binaryField(fieldInfo, value);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        visitor.stringField(fieldInfo, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        visitor.intField(fieldInfo, value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        visitor.longField(fieldInfo, value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        visitor.floatField(fieldInfo, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        visitor.doubleField(fieldInfo, value);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return visitor.needsField(fieldInfo);
    }
}
