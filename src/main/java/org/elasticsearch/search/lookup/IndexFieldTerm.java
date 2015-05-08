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

/**
 * Holds all information on a particular term in a field.
 * */
public interface IndexFieldTerm extends Iterable<TermPosition> {

    /**
     * get the term frequency within the current document
     */
    int tf() throws IOException;
    
    /** 
     * get the document frequency of the term 
     */
    long df() throws IOException;

    /** 
     * get the total term frequency of the term, that is, how often does the
     * term appear in any document?
     */
    long ttf() throws IOException;

    /**
     * sets the current document
     */
    void setDocument(int docId);

    /**
     * A user might decide inside a script to call get with _POSITIONS and then
     * a second time with _PAYLOADS. If the positions were recorded but the
     * payloads were not, the user will not have access to them. Therfore, throw
     * exception here explaining how to call get().
     */
    void validateFlags(int flags2);
}
