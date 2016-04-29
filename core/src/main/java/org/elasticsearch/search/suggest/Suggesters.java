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

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.search.suggest.completion.CompletionSuggester;
import org.elasticsearch.search.suggest.phrase.Laplace;
import org.elasticsearch.search.suggest.phrase.LinearInterpolation;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;
import org.elasticsearch.search.suggest.phrase.SmoothingModel;
import org.elasticsearch.search.suggest.phrase.StupidBackoff;
import org.elasticsearch.search.suggest.term.TermSuggester;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class Suggesters {
    private final Map<String, Suggester<?>> suggesters = new HashMap<>();
    private final NamedWriteableRegistry namedWriteableRegistry;

    public Suggesters(NamedWriteableRegistry namedWriteableRegistry) {
        this.namedWriteableRegistry = namedWriteableRegistry;
        register("phrase", PhraseSuggester.INSTANCE);
        register("term", TermSuggester.INSTANCE);
        register("completion", CompletionSuggester.INSTANCE);

        // Builtin smoothing models
        namedWriteableRegistry.register(SmoothingModel.class, Laplace.NAME, Laplace::new);
        namedWriteableRegistry.register(SmoothingModel.class, LinearInterpolation.NAME, LinearInterpolation::new);
        namedWriteableRegistry.register(SmoothingModel.class, StupidBackoff.NAME, StupidBackoff::new);
    }

    public void register(String key, Suggester<?> suggester) {
        if (suggesters.containsKey(key)) {
            throw new IllegalArgumentException("Can't register the same [suggester] more than once for [" + key + "]");
        }
        suggesters.put(key, suggester);
        namedWriteableRegistry.register(SuggestionBuilder.class, key, suggester);
    }

    public Suggester<?> getSuggester(String suggesterName) {
        Suggester<?> suggester = suggesters.get(suggesterName);
        if (suggester == null) {
            throw new IllegalArgumentException("suggester with name [" + suggesterName + "] not supported");
        }
        return suggester;
    }
}
