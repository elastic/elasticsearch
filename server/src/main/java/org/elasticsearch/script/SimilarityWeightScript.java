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

package org.elasticsearch.script;

import org.elasticsearch.index.similarity.ScriptedSimilarity;

/** A script that is used to compute scoring factors that are the same for all documents. */
public abstract class SimilarityWeightScript  {

    /** Compute the weight.
     * @param query  scoring factors that come from the query
     * @param field  field-level statistics
     * @param term   term-level statistics
     */
    public abstract double execute(ScriptedSimilarity.Query query, ScriptedSimilarity.Field field,
            ScriptedSimilarity.Term term);

    public interface Factory {
        SimilarityWeightScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] {"query", "field", "term"};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("similarity_weight", Factory.class);
}
