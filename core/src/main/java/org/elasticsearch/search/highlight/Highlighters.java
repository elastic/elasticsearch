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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * An extensions point and registry for all the highlighters a node supports.
 */
public class Highlighters extends ExtensionPoint.ClassMap<Highlighter> {

    private static final String FVH = "fvh";
    private static final String PLAIN = "plain";
    private static final String POSTINGS = "postings";

    private final Map<String, Highlighter> parsers;

    public Highlighters(){
        this(Collections.emptyMap());
    }

    private Highlighters(Map<String, Highlighter> parsers) {
        super("highlighter", Highlighter.class, new HashSet<>(Arrays.asList(FVH, PLAIN, POSTINGS)),
                Highlighters.class);
        this.parsers = Collections.unmodifiableMap(parsers);
    }

    @Inject
    public Highlighters(Settings settings, Map<String, Highlighter> parsers) {
        this(addBuiltIns(settings, parsers));
    }

    private static Map<String, Highlighter> addBuiltIns(Settings settings, Map<String, Highlighter> parsers) {
        Map<String, Highlighter> map = new HashMap<>();
        map.put(FVH,  new FastVectorHighlighter(settings));
        map.put(PLAIN, new PlainHighlighter());
        map.put(POSTINGS, new PostingsHighlighter());
        map.putAll(parsers);
        return map;
    }

    public Highlighter get(String type) {
        return parsers.get(type);
    }
}
