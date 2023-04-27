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
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Value fetcher that loads from doc values.
 */
// TODO rename this? It doesn't load from doc values, it loads from fielddata
// Which might be doc values, but might not be...
public final class DocValueFetcher implements ValueFetcher {
    private final DocValueFormat format;
    private final IndexFieldData<?> ifd;
    private FormattedDocValues formattedDocValues;
    private final StoredFieldsSpec storedFieldsSpec;

    public DocValueFetcher(DocValueFormat format, IndexFieldData<?> ifd, StoredFieldsSpec storedFieldsSpec) {
        this.format = format;
        this.ifd = ifd;
        this.storedFieldsSpec = storedFieldsSpec;
    }

    public DocValueFetcher(DocValueFormat format, IndexFieldData<?> ifd) {
        this(format, ifd, StoredFieldsSpec.NO_REQUIREMENTS);
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        formattedDocValues = ifd.load(context).getFormattedValues(format);
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        if (false == formattedDocValues.advanceExact(doc)) {
            return emptyList();
        }
        List<Object> result = new ArrayList<>(formattedDocValues.docValueCount());
        for (int i = 0, count = formattedDocValues.docValueCount(); i < count; ++i) {
            result.add(formattedDocValues.nextValue());
        }
        return result;
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return storedFieldsSpec;
    }
}
