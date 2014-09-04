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
package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.io.IOException;

/**
 * Defines a suggest command based on a prefix, typically to provide "auto-complete" functionality
 * for users as they type search terms. The implementation of the completion service uses FSTs that
 * are created at index-time and so must be defined in the mapping with the type "completion" before 
 * indexing.  
 */
public class CompletionSuggestionBuilder extends SuggestBuilder.SuggestionBuilder<CompletionSuggestionBuilder> {

    public CompletionSuggestionBuilder(String name) {
        super(name, "completion");
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }
}
