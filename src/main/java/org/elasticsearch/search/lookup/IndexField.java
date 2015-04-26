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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.CollectionStatistics;
import org.elasticsearch.common.util.MinimalMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Script interface to all information regarding a field.
 * */
public class IndexField extends MinimalMap<String, IndexFieldTerm> {

    /*
     * TermsInfo Objects that represent the Terms are stored in this map when
     * requested. Information such as frequency, doc frequency and positions
     * information can be retrieved from the TermInfo objects in this map.
     */
    private final Map<String, IndexFieldTerm> terms = new HashMap<>();

    // the name of this field
    private final String fieldName;

    /*
     * The holds the current reader. We need it to populate the field
     * statistics. We just delegate all requests there
     */
    private final LeafIndexLookup indexLookup;

    /*
     * General field statistics such as number of documents containing the
     * field.
     */
    private final CollectionStatistics fieldStats;

    /*
     * Represents a field in a document. Can be used to return information on
     * statistics of this field. Information on specific terms in this field can
     * be accessed by calling get(String term).
     */
    public IndexField(String fieldName, LeafIndexLookup indexLookup) throws IOException {

        assert fieldName != null;
        this.fieldName = fieldName;

        assert indexLookup != null;
        this.indexLookup = indexLookup;

        fieldStats = this.indexLookup.getIndexSearcher().collectionStatistics(fieldName);
    }

    /* get number of documents containing the field */
    public long docCount() throws IOException {
        return fieldStats.docCount();
    }

    /* get sum of the number of words over all documents that were indexed */
    public long sumttf() throws IOException {
        return fieldStats.sumTotalTermFreq();
    }

    /*
     * get the sum of doc frequencies over all words that appear in any document
     * that has the field.
     */
    public long sumdf() throws IOException {
        return fieldStats.sumDocFreq();
    }

    // TODO: might be good to get the field lengths here somewhere?

    /*
     * Returns a TermInfo object that can be used to access information on
     * specific terms. flags can be set as described in TermInfo.
     * 
     * TODO: here might be potential for running time improvement? If we knew in
     * advance which terms are requested, we could provide an array which the
     * user could then iterate over.
     */
    public IndexFieldTerm get(Object key, int flags) {
        String termString = (String) key;
        IndexFieldTerm indexFieldTerm = terms.get(termString);
        // see if we initialized already...
        if (indexFieldTerm == null) {
            indexFieldTerm = new IndexFieldTerm(termString, fieldName, indexLookup, flags);
            terms.put(termString, indexFieldTerm);
        }
        indexFieldTerm.validateFlags(flags);
        return indexFieldTerm;
    }

    /*
     * Returns a TermInfo object that can be used to access information on
     * specific terms. flags can be set as described in TermInfo.
     */
    @Override
    public IndexFieldTerm get(Object key) {
        // per default, do not initialize any positions info
        return get(key, IndexLookup.FLAG_FREQUENCIES);
    }

    public void setDocIdInTerms(int docId) {
        for (IndexFieldTerm ti : terms.values()) {
            ti.setDocument(docId);
        }
    }

}
