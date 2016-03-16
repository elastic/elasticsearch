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
package org.elasticsearch.search.suggest;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;
import org.elasticsearch.search.suggest.term.TermSuggester;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 *
 */
public final class Suggesters extends ExtensionPoint.ClassMap<Suggester> {
    private final Map<String, Suggester> parsers;

    public Suggesters() {
        this(Collections.emptyMap());
    }

    @Inject
    public Suggesters(Map<String, Suggester> suggesters) {
        super("suggester", Suggester.class, new HashSet<>(Arrays.asList("phrase", "term", "completion")), Suggesters.class, SuggestPhase.class);
        this.parsers = Collections.unmodifiableMap(addBuildIns(suggesters));
    }

    private static Map<String, Suggester> addBuildIns(Map<String, Suggester> suggesters) {
        final Map<String, Suggester> map = new HashMap<>();
        map.put("phrase", PhraseSuggester.PROTOTYPE);
        map.put("term", TermSuggester.PROTOTYPE);
        map.put("completion", CompletionSuggester.PROTOTYPE);
        map.putAll(suggesters);
        return map;
    }

    public SuggestionBuilder<? extends SuggestionBuilder> getSuggestionPrototype(String suggesterName) {
        Suggester<?> suggester = parsers.get(suggesterName);
        if (suggester == null) {
            throw new IllegalArgumentException("suggester with name [" + suggesterName + "] not supported");
        }
        SuggestionBuilder<?> suggestParser = suggester.getBuilderPrototype();
        if (suggestParser == null) {
            throw new IllegalArgumentException("suggester with name [" + suggesterName + "] not supported");
        }
        return suggestParser;
    }
}
