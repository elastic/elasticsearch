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

import java.io.IOException;

/**
 * Interface that allows adding custom implementations for providing
 * doc_count values for a document.
 */
public interface DocCountProvider {

    /**
     * Default implementation of a doc_count provider returns for every doc collected.
     */
    DocCountProvider DEFAULT_PROVIDER = doc -> 1;

    /** Return the number of doc_count for a specific document */
    int getDocCount(int doc) throws IOException;
}
