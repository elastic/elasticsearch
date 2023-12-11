/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;

public class SourceScoreOrderFragmentsBuilder extends ScoreOrderFragmentsBuilder {

    private final FetchContext fetchContext;
    private final MappedFieldType fieldType;
    private final Source source;
    private final ValueFetcher valueFetcher;
    private final boolean fixBrokenAnalysis;

    public SourceScoreOrderFragmentsBuilder(
        MappedFieldType fieldType,
        FetchContext fetchContext,
        boolean fixBrokenAnalysis,
        Source source,
        String[] preTags,
        String[] postTags,
        BoundaryScanner boundaryScanner
    ) {
        super(preTags, postTags, boundaryScanner);
        this.fetchContext = fetchContext;
        this.fieldType = fieldType;
        this.source = source;
        this.valueFetcher = fieldType.valueFetcher(fetchContext.getSearchExecutionContext(), null);
        this.fixBrokenAnalysis = fixBrokenAnalysis;
    }

    @Override
    protected Field[] getFields(IndexReader reader, int docId, String fieldName) throws IOException {
        // we know its low level reader, and matching docId, since that's how we call the highlighter with
        return SourceSimpleFragmentsBuilder.doGetFields(docId, valueFetcher, source, fetchContext, fieldType);
    }

    @Override
    protected String makeFragment(
        StringBuilder buffer,
        int[] index,
        Field[] values,
        WeightedFragInfo fragInfo,
        String[] preTags,
        String[] postTags,
        Encoder encoder
    ) {
        if (fixBrokenAnalysis) {
            fragInfo = FragmentBuilderHelper.fixWeightedFragInfo(fragInfo);
        }
        return super.makeFragment(buffer, index, values, fragInfo, preTags, postTags, encoder);
    }
}
