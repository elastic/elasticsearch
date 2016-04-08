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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.IOException;

public abstract class Suggester<T extends SuggestionSearchContext.SuggestionContext> implements Writeable.Reader<SuggestionBuilder<?>> {

    protected abstract Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>
        innerExecute(String name, T suggestion, IndexSearcher searcher, CharsRefBuilder spare) throws IOException;

    /**
     * Read the SuggestionBuilder paired with this Suggester XContent.
     */
    public abstract SuggestionBuilder<?> innerFromXContent(QueryParseContext context) throws IOException;

    public Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>
        execute(String name, T suggestion, IndexSearcher searcher, CharsRefBuilder spare) throws IOException {
        // #3469 We want to ignore empty shards

        if (searcher.getIndexReader().numDocs() == 0) {
            return null;
        }
        return innerExecute(name, suggestion, searcher, spare);
    }

}
