/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.elasticsearch.index.mapper.MappedFieldType;

/**
 * Direct Subclass of Lucene's org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder
 * that corrects offsets for broken analysis chains.
 */
public class SimpleFragmentsBuilder extends org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder {

    protected final MappedFieldType fieldType;
    private final boolean fixBrokenAnalysis;

    public SimpleFragmentsBuilder(MappedFieldType fieldType,
                                  boolean fixBrokenAnalysis,
                                  String[] preTags,
                                  String[] postTags,
                                  BoundaryScanner boundaryScanner) {
        super(preTags, postTags, boundaryScanner);
        this.fieldType = fieldType;
        this.fixBrokenAnalysis = fixBrokenAnalysis;
    }

    @Override
    protected String makeFragment( StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo,
            String[] preTags, String[] postTags, Encoder encoder) {
        if (fixBrokenAnalysis) {
            fragInfo = FragmentBuilderHelper.fixWeightedFragInfo(fragInfo);
        }
        return super.makeFragment(buffer, index, values, fragInfo, preTags, postTags, encoder);
   }
}
