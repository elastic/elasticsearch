/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Value fetcher that loads from doc values.
 */
public final class DocValueFetcher implements ValueFetcher {
    private final DocValueFormat format;
    private final IndexFieldData<?> ifd;
    private FormattedDocValues formattedDocValues;

    public DocValueFetcher(DocValueFormat format, IndexFieldData<?> ifd) {
        this.format = format;
        this.ifd = ifd;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        formattedDocValues = ifd.load(context).getFormattedValues(format);
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup) throws IOException {
        if (false == formattedDocValues.advanceExact(lookup.docId())) {
            return emptyList();
        }
        List<Object> result = new ArrayList<>(formattedDocValues.docValueCount());
        for (int i = 0, count = formattedDocValues.docValueCount(); i < count; ++i) {
            result.add(formattedDocValues.nextValue());
        }
        return result;
    }

}
