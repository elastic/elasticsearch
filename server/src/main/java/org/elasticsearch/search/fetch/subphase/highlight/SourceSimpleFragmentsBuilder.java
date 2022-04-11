/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

public class SourceSimpleFragmentsBuilder extends SimpleFragmentsBuilder {

    private final SourceLookup sourceLookup;

    public SourceSimpleFragmentsBuilder(
        MappedFieldType fieldType,
        boolean fixBrokenAnalysis,
        SourceLookup sourceLookup,
        String[] preTags,
        String[] postTags,
        BoundaryScanner boundaryScanner
    ) {
        super(fieldType, fixBrokenAnalysis, preTags, postTags, boundaryScanner);
        this.sourceLookup = sourceLookup;
    }

    public static final Field[] EMPTY_FIELDS = new Field[0];

    @Override
    protected Field[] getFields(IndexReader reader, int docId, String fieldName) throws IOException {
        // we know its low level reader, and matching docId, since that's how we call the highlighter with
        List<Object> values = sourceLookup.extractRawValues(fieldType.name());
        if (values.isEmpty()) {
            return EMPTY_FIELDS;
        }
        Field[] fields = new Field[values.size()];
        for (int i = 0; i < values.size(); i++) {
            fields[i] = new Field(fieldType.name(), values.get(i).toString(), TextField.TYPE_NOT_STORED);
        }
        return fields;
    }

}
