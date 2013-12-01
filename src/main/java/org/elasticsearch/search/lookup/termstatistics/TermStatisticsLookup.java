/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.lookup.termstatistics;

import com.google.common.collect.ImmutableMap.Builder;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.util.MinimalMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TermStatisticsLookup extends MinimalMap {

    /**
     * Flag to pass to {@link ScriptField#get(String, flags)} if you require
     * offsets in the returned {@link ScriptTerm}.
     */
    public static final int FLAG_OFFSETS = 2;

    /**
     * Flag to pass to {@link ScriptField#get(String, flags)} if you require
     * payloads in the returned {@link ScriptTerm}.
     */
    public static final int FLAG_PAYLOADS = 4;

    /**
     * Flag to pass to {@link ScriptField#get(String, flags)} if you require
     * frequencies in the returned {@link ScriptTerm}. Frequencies might be
     * returned anyway for some lucene codecs even if this flag is no set.
     */
    public static final int FLAG_FREQUENCIES = 8;

    /**
     * Flag to pass to {@link ScriptField#get(String, flags)} if you require
     * positions in the returned {@link ScriptTerm}.
     */
    public static final int FLAG_POSITIONS = 16;

    /**
     * Flag to pass to {@link ScriptField#get(String, flags)} if you require
     * positions in the returned {@link ScriptTerm}.
     */
    public static final int FLAG_DO_NOT_RECORD = 32;

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
    final private Map<String, ScriptField> scriptFields = new HashMap<String, ScriptField>();

    // number of documents per shard. cached here because the computation is
    // expensive
    private int numDocs = -1;

    // number of deleted documents per shard. cached here because the
    // computation is expensive
    private int numDeletedDocs = -1;

    public int numDocs() {
        if (numDocs == -1) {
            numDocs = parentReader.numDocs();
        }
        return numDocs;
    }

    public int numDeletedDocs() {
        if (numDeletedDocs == -1) {
            numDeletedDocs = parentReader.numDeletedDocs();
        }
        return numDeletedDocs;
    }

    public TermStatisticsLookup(Builder<String, Object> builder) {
        builder.put("_FREQUENCIES", TermStatisticsLookup.FLAG_FREQUENCIES);
        builder.put("_POSITIONS", TermStatisticsLookup.FLAG_POSITIONS);
        builder.put("_OFFSETS", TermStatisticsLookup.FLAG_OFFSETS);
        builder.put("_PAYLOADS", TermStatisticsLookup.FLAG_PAYLOADS);
        builder.put("_DO_NOT_RECORD", TermStatisticsLookup.FLAG_DO_NOT_RECORD);
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
                // scriptFields.clear() here instead of assertion just to be on
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
        for (ScriptField stat : scriptFields.values()) {
            stat.setReader();
        }
    }

    public void setNextDocId(int docId) {
        if (this.docId == docId) { // if we are called with the same docId,
                                   // nothing to do
            return;
        }
        // We assume that docs are processed in ascending order of id. If this
        // is not the case, we would have to re initialize all posting lists in
        // ScriptTerm. TODO: Instead of assert we could also call
        // setReaderInFields(); here?
        if (this.docId > docId) {
            // This might happen if the same SearchLookup is used in different
            // phases, such as score and fetch phase.
            // In this case we do not want to re initialize posting list etc.
            // because we do not even know if term and field statistics will be
            // needed in this new phase.
            // Therefore we just remove all ScriptFields.
            scriptFields.clear();
        }
        this.docId = docId;
        setNextDocIdInFields();
    }

    protected void setNextDocIdInFields() {
        for (ScriptField stat : scriptFields.values()) {
            stat.setDocIdInTerms();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        // we always return information on a field - if the field does not exist
        // everything will be 0/null
        return true;
    }

    @Override
    public ScriptField get(Object key) {
        String stringField = (String) key;
        ScriptField scriptField = lookupScriptField(key);
        if (scriptField == null) {
            try {
                scriptField = new ScriptField(stringField, this);
                putScriptField(stringField, scriptField);
            } catch (IOException e) {
                throw new ElasticSearchException(e.getMessage());
            }
        }
        return scriptField;
    }

    private void putScriptField(String stringField, ScriptField scriptField) {
        scriptFields.put(stringField, scriptField);
    }

    private ScriptField lookupScriptField(Object key) {
        return scriptFields.get(key);
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
