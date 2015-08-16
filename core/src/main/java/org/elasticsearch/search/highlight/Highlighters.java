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
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;

import java.util.*;

/**
 * An extensions point and registry for all the highlighters a node supports.
 */
public class Highlighters extends ExtensionPoint.ClassMap<Highlighter> {

    @Deprecated // remove in 3.0
    private static final String FAST_VECTOR_HIGHLIGHTER = "fast-vector-highlighter";
    private static final String FVH = "fvh";
    @Deprecated // remove in 3.0
    private static final String HIGHLIGHTER = "highlighter";
    private static final String PLAIN = "plain";
    @Deprecated // remove in 3.0
    private static final String POSTINGS_HIGHLIGHTER = "postings-highlighter";
    private static final String POSTINGS = "postings";


    private final Map<String, Highlighter> parsers;
    private final DeprecationLogger deprecationLogger = new DeprecationLogger(ESLoggerFactory.getLogger(Highlighters.class.getName()));

    public Highlighters(){
        this(Collections.EMPTY_MAP);
    }

    private Highlighters(Map<String, Highlighter> parsers) {
        super("highlighter", Highlighter.class, new HashSet<>(Arrays.asList(FVH, FAST_VECTOR_HIGHLIGHTER, PLAIN, HIGHLIGHTER, POSTINGS, POSTINGS_HIGHLIGHTER)),
                Highlighters.class);
        this.parsers = Collections.unmodifiableMap(parsers);
    }

    @Inject
    public Highlighters(Settings settings, Map<String, Highlighter> parsers) {
        this(addBuiltIns(settings, parsers));
    }

    private static Map<String, Highlighter> addBuiltIns(Settings settings, Map<String, Highlighter> parsers) {
        // build in highlighers
        Map<String, Highlighter> map = new HashMap<>();
        map.put(FVH,  new FastVectorHighlighter(settings));
        map.put(FAST_VECTOR_HIGHLIGHTER, map.get(FVH));
        map.put(PLAIN, new PlainHighlighter());
        map.put(HIGHLIGHTER, map.get(PLAIN));
        map.put(POSTINGS, new PostingsHighlighter());
        map.put(POSTINGS_HIGHLIGHTER, map.get(POSTINGS));
        map.putAll(parsers);
        return map;
    }

    public Highlighter get(String type) {
        switch (type) {
            case FAST_VECTOR_HIGHLIGHTER:
                deprecationLogger.deprecated("highlighter key [{}] is deprecated and will be removed in 3.x use [{}] instead", FAST_VECTOR_HIGHLIGHTER, FVH);
                break;
            case HIGHLIGHTER:
                deprecationLogger.deprecated("highlighter key [{}] is deprecated and will be removed in 3.x use [{}] instead", HIGHLIGHTER, PLAIN);
                break;
            case POSTINGS_HIGHLIGHTER:
                deprecationLogger.deprecated("highlighter key [{}] is deprecated and will be removed in 3.x use [{}] instead", POSTINGS_HIGHLIGHTER, POSTINGS);
                break;
        }
        return parsers.get(type);
    }

}
