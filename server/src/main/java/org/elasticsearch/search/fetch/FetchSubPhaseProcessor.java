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

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;

import java.io.IOException;

/**
 * Executes the logic for a {@link FetchSubPhase} against a particular leaf reader and hit
 */
public interface FetchSubPhaseProcessor {

    /**
     * Called when moving to the next {@link LeafReaderContext} for a set of hits
     */
    void setNextReader(LeafReaderContext readerContext) throws IOException;

    /**
     * Called in doc id order for each hit in a leaf reader
     */
    void process(HitContext hitContext) throws IOException;

}
