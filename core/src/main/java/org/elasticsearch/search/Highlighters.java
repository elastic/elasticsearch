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
package org.elasticsearch.search;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.highlight.FastVectorHighlighter;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.highlight.PostingsHighlighter;

import java.util.HashMap;
import java.util.Map;

/**
 * An extensions point and registry for all the highlighters a node supports.
 */
public final class Highlighters {

    private final Map<String, Highlighter> parsers = new HashMap<>();

    public Highlighters(Settings settings) {
        registerHighlighter("fvh",  new FastVectorHighlighter(settings));
        registerHighlighter("plain", new PlainHighlighter());
        registerHighlighter("postings", new PostingsHighlighter());
    }

    /**
     * Returns the highlighter for the given key or <code>null</code> if there is no highlighter registered for that key.
     */
    public Highlighter get(String key) {
        return parsers.get(key);
    }

    /**
     * Registers a highlighter for the given key
     * @param key the key the highlighter should be referenced by in the search request
     * @param highlighter the highlighter instance
     */
    void registerHighlighter(String key, Highlighter highlighter) {
        if (highlighter == null) {
            throw new IllegalArgumentException("Can't register null highlighter for key: [" + key + "]");
        }
        if (parsers.putIfAbsent(key, highlighter) != null) {
            throw new IllegalArgumentException("Can't register the same [highlighter] more than once for [" + key + "]");
        }
    }
}
