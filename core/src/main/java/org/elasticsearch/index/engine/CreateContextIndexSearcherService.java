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
import org.elasticsearch.index.engine.Engine.Searcher;

/**
 * The service is responsible for two things:
 * 1) Create a new ContextIndexSearcher instance for each shard level operation (search, get, field stats etc.)
 * 2) Optionally wrapping the {@link DirectoryReader} and {@link IndexSearcher} of a {@link Searcher} via the
 * configured {@link IndexSearcherWrapper} instance. This allows custom functionally to be added the {@link Searcher}
 * before being used to do an operation (search, get, field stats etc.)
 */
// TODO: This needs extension point is a bit hacky now, because the IndexSearch from the engine can only be wrapped once,
// if we allowed the IndexSearcher to be wrapped multiple times then a custom IndexSearcherWrapper needs have good
// control over its location in the wrapping chain
public interface CreateContextIndexSearcherService {

    /**
     * If there are configured {@link IndexSearcherWrapper} instances, the {@link IndexSearcher} of the provided engine searcher
     * gets wrapped and a new {@link Searcher} instances is returned, otherwise the provided {@link Searcher} is returned.
     *
     * This is invoked each time a {@link Searcher} is requested to do an operation. (for example search)
     */
    Searcher wrap(EngineConfig engineConfig, final Searcher originalEngineSearcher) throws EngineException;

}
