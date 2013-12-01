/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.lookup.termstatistics;

import org.apache.lucene.search.CollectionStatistics;
import org.elasticsearch.common.util.MinimalMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Script interface to all information regarding a field.
 * */
public class ScriptField extends MinimalMap {

    /*
     * TermsInfo Objects that represent the Terms are stored in this map when
     * requested. Information such as frequency, doc frequency and positions
     * information can be retrieved from the TermInfo objects in this map.
     */
    final private Map<String, ScriptTerm> terms = new HashMap<String, ScriptTerm>();

    // the name of this field
    String fieldName;

    /*
     * The holds the current reader. We need it to populate the field
     * statistics. We just delegate all requests there
     */
    private TermStatisticsLookup indexStats;

    /*
     * General field statistics such as number of documents containing the
     * field.
     */
    private CollectionStatistics fieldStats;

    private boolean fieldStatsInitialized = false;

    private void initFieldStats() throws IOException {
        if (fieldStatsInitialized == false) {
            fieldStats = indexStats.getIndexSearcher().collectionStatistics(fieldName);
            fieldStatsInitialized = true;
        }
    }

    /*
     * Uodate posting lists in all TermInfo objects
     */
    void setReader() {
        for (ScriptTerm ti : terms.values()) {
            ti.setNextReader();
        }
    }

    /*
     * Represents a field in a document. Can be used to return information on
     * statistics of this field. Information on specific terms in this field can
     * be accessed by calling get(String term).
     */
    public ScriptField(String fieldName, TermStatisticsLookup indexStats) throws IOException {

        assert fieldName != null;
        this.fieldName = fieldName;

        assert indexStats != null;
        this.indexStats = indexStats;
    }

    /* get number of documents containing the field */
    public long docCount() throws IOException {
        initFieldStats();
        return fieldStats.docCount();
    }

    /* get sum of the number of words over all documents that were indexed */
    public long sumttf() throws IOException {
        initFieldStats();
        return fieldStats.sumTotalTermFreq();
    }

    /*
     * get the sum of doc frequencies over all words that appear in any document
     * that has the field.
     */
    public long sumdf() throws IOException {
        initFieldStats();
        return fieldStats.sumDocFreq();
    }

    // TODO: might be good to get the field lengths here somewhere?

    /*
     * Returns a TermInfo object that can be used to access information on
     * specific terms. flags can be set as described in TermInfo.
     */
    public ScriptTerm get(Object key, int flags) {
        String termString = (String) key;
        ScriptTerm termInfo = lookupTermInfo(termString);
        // see if we initialized already...
        if (termInfo == null) {
            termInfo = new ScriptTerm(termString, fieldName, indexStats, flags);
            putTermInfo(termString, termInfo);
        }
        termInfo.validateFlags(flags);
        return termInfo;
    }

    private void putTermInfo(String termString, ScriptTerm termInfo) {
        terms.put(termString, termInfo);
    }

    /*
     * TODO: here might be potential for running time improvement? If we knew in
     * advance which terms are requested, we could provide an array which the
     * user could then iterate over.
     */
    private ScriptTerm lookupTermInfo(String termString) {
        return terms.get(termString);
    }

    /*
     * Returns a TermInfo object that can be used to access information on
     * specific terms. flags can be set as described in TermInfo.
     */
    public ScriptTerm get(Object key) {
        // per default, do not initialize any positions info
        return get(key, TermStatisticsLookup.FLAG_FREQUENCIES);
    }

    public boolean containsKey(Object key) {
        return true;
    }

    public void setDocIdInTerms() {
        for (ScriptTerm ti : terms.values()) {
            ti.setNextDoc();
        }
    }

}
