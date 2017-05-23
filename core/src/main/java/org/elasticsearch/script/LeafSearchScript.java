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

import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.lucene.ScorerAware;

import java.util.Map;

/**
 * A per-segment {@link SearchScript}.
 *
 * This is effectively a functional interface, requiring at least implementing {@link #runAsDouble()}.
 */
public interface LeafSearchScript extends ScorerAware, ExecutableScript {

    /**
     * Set the document this script will process next.
     */
    default void setDocument(int doc) {}

    @Override
    default void setScorer(Scorer scorer) {}

    /**
     * Set the source for the current document.
     */
    default void setSource(Map<String, Object> source) {}
    
    /**
     * Sets per-document aggregation {@code _value}.
     * <p>
     * The default implementation just calls {@code setNextVar("_value", value)} but
     * some engines might want to handle this differently for better performance.
     * <p>
     * @param value per-document value, typically a String, Long, or Double
     */
    default void setNextAggregationValue(Object value) {
        setNextVar("_value", value);
    }

    @Override
    default void setNextVar(String field, Object value) {}

    /**
     * Return the result as a long. This is used by aggregation scripts over long fields.
     */
    default long runAsLong() {
        throw new UnsupportedOperationException("runAsLong is not implemented");
    }

    @Override
    default Object run() {
        return runAsDouble();
    }

    /**
     * Return the result as a double. This is the main use case of search script, used for document scoring.
     */
    double runAsDouble();
}
