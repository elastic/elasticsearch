/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.lookup;

import com.google.common.collect.ImmutableMap.Builder;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.MinimalMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IndexLookup extends MinimalMap<String, IndexField> {

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * offsets in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_OFFSETS = 2;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * payloads in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_PAYLOADS = 4;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * frequencies in the returned {@link IndexFieldTerm}. Frequencies might be
     * returned anyway for some lucene codecs even if this flag is no set.
     */
    public static final int FLAG_FREQUENCIES = 8;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * positions in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_POSITIONS = 16;

    /**
     * Flag to pass to {@link IndexField#get(String, flags)} if you require
     * positions in the returned {@link IndexFieldTerm}.
     */
    public static final int FLAG_CACHE = 32;

    // Current reader from which we can get the term vectors. No info on term
    // and field statistics.
    private AtomicReader reader;

    // The parent reader from which we can get proper field and term
    // statistics
    private CompositeReader parentReader;

    // we need this later to get the field and term statistics of the shard
    private IndexSearcher indexSearcher;

    // we need this later to get the term statistics of the shard
    private IndexReaderContext indexReaderContext;

    // current docId
    private int docId = -1;

    // stores the objects that are used in the script. we maintain this map
    // because we do not want to re-initialize the objects each time a field is
    // accessed
    private final Map<String, IndexField> indexFields = new HashMap<>();

    // number of documents per shard. cached here because the computation is
    // expensive
    private int numDocs = -1;

    // the maximum doc number of the shard.
    private int maxDoc = -1;

    // number of deleted documents per shard. cached here because the
    // computation is expensive
    private int numDeletedDocs = -1;

    public int numDocs() {
        if (numDocs == -1) {
            numDocs = parentReader.numDocs();
        }
        return numDocs;
    }

    public int maxDoc() {
        if (maxDoc == -1) {
            maxDoc = parentReader.maxDoc();
        }
        return maxDoc;
    }

    public int numDeletedDocs() {
        if (numDeletedDocs == -1) {
            numDeletedDocs = parentReader.numDeletedDocs();
        }
        return numDeletedDocs;
    }

    public IndexLookup(Builder<String, Object> builder) {
        builder.put("_FREQUENCIES", IndexLookup.FLAG_FREQUENCIES);
        builder.put("_POSITIONS", IndexLookup.FLAG_POSITIONS);
        builder.put("_OFFSETS", IndexLookup.FLAG_OFFSETS);
        builder.put("_PAYLOADS", IndexLookup.FLAG_PAYLOADS);
        builder.put("_CACHE", IndexLookup.FLAG_CACHE);
    }

    public void setNextReader(AtomicReaderContext context) {
        if (reader == context.reader()) { // if we are called with the same
                                          // reader, nothing to do
            return;
        }
        // check if we have to invalidate all field and shard stats - only if
        // parent reader changed
        if (context.parent != null) {
            if (parentReader == null) {
                parentReader = context.parent.reader();
                indexSearcher = new IndexSearcher(parentReader);
                indexReaderContext = context.parent;
            } else {
                // parent reader may only be set once. TODO we could also call
                // indexFields.clear() here instead of assertion just to be on
                // the save side
                assert (parentReader == context.parent.reader());
            }
        } else {
            assert parentReader == null;
        }
        reader = context.reader();
        docId = -1;
        setReaderInFields();
    }

    protected void setReaderInFields() {
        for (IndexField stat : indexFields.values()) {
            stat.setReader(reader);
        }
    }

    public void setNextDocId(int docId) {
        if (this.docId == docId) { // if we are called with the same docId,
                                   // nothing to do
            return;
        }
        // We assume that docs are processed in ascending order of id. If this
        // is not the case, we would have to re initialize all posting lists in
        // IndexFieldTerm. TODO: Instead of assert we could also call
        // setReaderInFields(); here?
        if (this.docId > docId) {
            // This might happen if the same SearchLookup is used in different
            // phases, such as score and fetch phase.
            // In this case we do not want to re initialize posting list etc.
            // because we do not even know if term and field statistics will be
            // needed in this new phase.
            // Therefore we just remove all IndexFieldTerms.
            indexFields.clear();
        }
        this.docId = docId;
        setNextDocIdInFields();
    }

    protected void setNextDocIdInFields() {
        for (IndexField stat : indexFields.values()) {
            stat.setDocIdInTerms(this.docId);
        }
    }

    /*
     * TODO: here might be potential for running time improvement? If we knew in
     * advance which terms are requested, we could provide an array which the
     * user could then iterate over.
     */
    @Override
    public IndexField get(Object key) {
        String stringField = (String) key;
        IndexField indexField = indexFields.get(key);
        if (indexField == null) {
            try {
                indexField = new IndexField(stringField, this);
                indexFields.put(stringField, indexField);
            } catch (IOException e) {
                throw new ElasticsearchException(e.getMessage());
            }
        }
        return indexField;
    }

    /*
     * Get the lucene term vectors. See
     * https://lucene.apache.org/core/4_0_0/core/org/apache/lucene/index/Fields.html
     * *
     */
    public Fields termVectors() throws IOException {
        assert reader != null;
        return reader.getTermVectors(docId);
    }

    AtomicReader getReader() {
        return reader;
    }

    public int getDocId() {
        return docId;
    }

    public IndexReader getParentReader() {
        if (parentReader == null) {
            return reader;
        }
        return parentReader;
    }

    public IndexSearcher getIndexSearcher() {
        if (indexSearcher == null) {
            return new IndexSearcher(reader);
        }
        return indexSearcher;
    }

    public IndexReaderContext getReaderContext() {
        return indexReaderContext;
    }
}
