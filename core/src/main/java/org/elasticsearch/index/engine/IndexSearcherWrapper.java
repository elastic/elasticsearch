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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;

/**
 * Extension point to add custom functionality at request time to the {@link DirectoryReader}
 * and {@link IndexSearcher} managed by the {@link Engine}.
 */
public interface IndexSearcherWrapper {

    /**
     * @param reader The provided directory reader to be wrapped to add custom functionality
     * @return a new directory reader wrapping the provided directory reader or if no wrapping was performed
     *         the provided directory reader
     */
    DirectoryReader wrap(DirectoryReader reader);

    /**
     * @param engineConfig  The engine config which can be used to get the query cache and query cache policy from
     *                      when creating a new index searcher
     * @param searcher      The provided index searcher to be wrapped to add custom functionality
     * @return a new index searcher wrapping the provided index searcher or if no wrapping was performed
     *         the provided index searcher
     */
    IndexSearcher wrap(EngineConfig engineConfig, IndexSearcher searcher) throws EngineException;

}
