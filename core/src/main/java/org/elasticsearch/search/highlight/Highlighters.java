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
package org.elasticsearch.search.highlight;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class Highlighters {

    private final Map<String, Highlighter> parsers;

    @Inject
    public Highlighters(Settings settings, Set<Highlighter> parsers) {
        // build in highlighers
        Map<String, Highlighter> map = new HashMap<>();
        add(map, new FastVectorHighlighter(settings));
        add(map, new PlainHighlighter());
        add(map, new PostingsHighlighter());
        for (Highlighter highlighter : parsers) {
            add(map, highlighter);
        }
        this.parsers = Collections.unmodifiableMap(map);
    }

    public Highlighter get(String type) {
        return parsers.get(type);
    }

    private void add(Map<String, Highlighter> map, Highlighter highlighter) {
        for (String type : highlighter.names()) {
            map.put(type, highlighter);
        }
    }

}
