/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;

public abstract class FilterStoredFieldsReader extends StoredFieldsReader {

    protected final StoredFieldsReader in;

    public FilterStoredFieldsReader(StoredFieldsReader fieldsReader) {
        this.in = fieldsReader;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        in.document(docID, visitor);
    }

    @Override
    public abstract StoredFieldsReader clone();

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }

    public static StoredFieldsReader unwrap(StoredFieldsReader storedFieldsReader) {
        while (storedFieldsReader instanceof FilterStoredFieldsReader filterStoredFieldsReader) {
            storedFieldsReader = filterStoredFieldsReader.in;
        }
        return storedFieldsReader;
    }
}
