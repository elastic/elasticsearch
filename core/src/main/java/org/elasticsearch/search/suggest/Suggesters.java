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

import java.util.Map;

/**
 * Registry of Suggesters. This is only its own class to make Guice happy.
 */
public final class Suggesters {
    private final Map<String, Suggester<?>> suggesters;

    public Suggesters(Map<String, Suggester<?>> suggesters) {
        this.suggesters = suggesters;
    }

    public Suggester<?> getSuggester(String suggesterName) {
        Suggester<?> suggester = suggesters.get(suggesterName);
        if (suggester == null) {
            throw new IllegalArgumentException("suggester with name [" + suggesterName + "] not supported");
        }
        return suggester;
    }
}
