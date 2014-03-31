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
package org.elasticsearch.search.suggest.freetext;

import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

/**
 *
 */
public class FreeTextSuggestionContext extends SuggestionSearchContext.SuggestionContext {

    private FieldMapper<?> mapper;
    private String separator = null;

    public FreeTextSuggestionContext(Suggester suggester) {
        super(suggester);
    }

    public FieldMapper<?> mapper() {
        return this.mapper;
    }

    public void mapper(FieldMapper<?> mapper) {
        this.mapper = mapper;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getSeparator() {
        return separator;
    }
}
