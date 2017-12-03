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

package org.elasticsearch.search.internal;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.search.Scroll;

import java.util.HashMap;
import java.util.Map;

/** Wrapper around information that needs to stay around when scrolling. */
public final class ScrollContext {

    private Map<String, Object> context = null;

    public long totalHits = -1;
    public float maxScore;
    public ScoreDoc lastEmittedDoc;
    public Scroll scroll;

    /**
     * Returns the object or <code>null</code> if the given key does not have a
     * value in the context
     */
    @SuppressWarnings("unchecked") // (T)object
    public <T> T getFromContext(String key) {
        return context != null ? (T) context.get(key) : null;
    }

    /**
     * Puts the object into the context
     */
    public void putInContext(String key, Object value) {
        if (context == null) {
            context = new HashMap<>();
        }
        context.put(key, value);
    }
}
