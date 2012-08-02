/*
 * Licensed to Elastic Search under one
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
package org.elasticsearch.script;

import org.apache.lucene.search.Explanation;

/**
 * To be implemented by {@link SearchScript} which can provided an {@link Explanation} of the score
 */
public interface ExplainableSearchScript extends SearchScript {

    /**
     * Build the explanation of the current document being scored
     * 
     * @param subQueryExpl the explanation of the subQuery 
     */
    Explanation explain(Explanation subQueryExpl);

}
