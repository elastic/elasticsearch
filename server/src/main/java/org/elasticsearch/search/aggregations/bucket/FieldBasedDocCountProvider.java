/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

import java.io.IOException;

/**
 * An implementation of a doc_count provider that reads the doc_count value
 * in a doc value field in the document. If a document has no doc_count field
 * the implementation will return 1 as the default value.
 */
public class FieldBasedDocCountProvider implements DocCountProvider {

    private final String docCountFieldName;
    private NumericDocValues docCountValues;

    public FieldBasedDocCountProvider(String docCountFieldName) {
        this.docCountFieldName = docCountFieldName;
    }

    @Override
    public int getDocCount(int doc) throws IOException {
        if (docCountValues != null && docCountValues.advanceExact(doc)) {
            return (int) docCountValues.longValue();
        } else {
            return 1;
        }
    }

    @Override
    public void setLeafReaderContext(LeafReaderContext ctx) {
        try {
            docCountValues = DocValues.getNumeric(ctx.reader(), docCountFieldName);
        } catch (IOException e) {
            docCountValues = null;
        }
    }

}
