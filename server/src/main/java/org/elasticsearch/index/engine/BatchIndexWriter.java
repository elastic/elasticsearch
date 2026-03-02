/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;

import java.io.IOException;

public class BatchIndexWriter {

    private final IndexWriter indexWriter;

    public BatchIndexWriter(IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }

    public long batchAddDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
        return indexWriter.addDocuments(docs);
    }
}
