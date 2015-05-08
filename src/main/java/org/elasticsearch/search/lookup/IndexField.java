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

import java.io.IOException;
import java.util.Map;

/**
 * Script interface to all information regarding a field.
 * */
public interface IndexField extends Map<String, IndexFieldTerm> {

    /** get number of documents containing the field */
    long docCount() throws IOException;

    /** get sum of the number of words over all documents that were indexed */
    long sumttf() throws IOException;

    /**
     * get the sum of doc frequencies over all words that appear in any document
     * that has the field.
     */
    long sumdf() throws IOException;

    /**
     * Returns a TermInfo object that can be used to access information on
     * specific terms. flags can be set as described in TermInfo.
     * 
     * TODO: here might be potential for running time improvement? If we knew in
     * advance which terms are requested, we could provide an array which the
     * user could then iterate over.
     */
    IndexFieldTerm get(Object key, int flags);

    void setDocIdInTerms(int docId);
}
