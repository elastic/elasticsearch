/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.List;

/**
 * Utility class for building highlighting queries for the purpose of extracting snippets.
 */
public class HighlightSnippetUtils {

    public static SearchHighlightContext buildSearchHighlightContextForSnippets(
        SearchExecutionContext searchExecutionContext,
        String field,
        int numSnippets,
        int snippetCharLength,
        QueryBuilder queryBuilder
    ) throws IOException {
        SearchHighlightContext.Field highlightField = buildFieldHighlightContextForSnippets(
            searchExecutionContext,
            field,
            numSnippets,
            snippetCharLength,
            queryBuilder.toQuery(searchExecutionContext)
        );
        return new SearchHighlightContext(List.of(highlightField));
    }

    public static SearchHighlightContext.Field buildFieldHighlightContextForSnippets(
        SearchExecutionContext searchExecutionContext,
        String fieldName,
        int numSnippets,
        int snippetCharLength,
        Query query
    ) {
        SearchHighlightContext.FieldOptions.Builder optionsBuilder = new SearchHighlightContext.FieldOptions.Builder();
        optionsBuilder.numberOfFragments(numSnippets);
        optionsBuilder.fragmentCharSize(snippetCharLength);
        // Note: The default SENTENCE boundary scanner used by the DefaultHighlighter will return fragments larger than the specified
        // snippetLength. This has implications when appending and calculating ByteArrays, so we specify WORD.
        optionsBuilder.boundaryScannerType(HighlightBuilder.BoundaryScannerType.WORD);
        optionsBuilder.noMatchSize(snippetCharLength);
        optionsBuilder.preTags(new String[] { "" });
        optionsBuilder.postTags(new String[] { "" });
        optionsBuilder.requireFieldMatch(false);
        optionsBuilder.scoreOrdered(true);
        optionsBuilder.highlightQuery(query);
        return new SearchHighlightContext.Field(fieldName, optionsBuilder.build());
    }

}
