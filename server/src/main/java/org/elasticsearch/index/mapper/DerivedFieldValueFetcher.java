/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.annotation.PublicApi;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * The value fetcher contains logic to execute script and fetch the value in form of list of object.
 * It expects DerivedFieldScript.LeafFactory as an input and sets the contract with consumer to call
 * {@link #setNextReader(LeafReaderContext)} whenever a segment is switched.
 */
@PublicApi(since = "2.14.0")
public class DerivedFieldValueFetcher implements ValueFetcher {
    private DerivedFieldScript derivedFieldScript;
    private final DerivedFieldScript.LeafFactory derivedFieldScriptFactory;

    private final Function<Object, Object> valueForDisplay;

    public DerivedFieldValueFetcher(DerivedFieldScript.LeafFactory derivedFieldScriptFactory, Function<Object, Object> valueForDisplay) {
        this.derivedFieldScriptFactory = derivedFieldScriptFactory;
        this.valueForDisplay = valueForDisplay;
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup) {
        List<Object> values = fetchValuesInternal(lookup);
        if (values.isEmpty()) {
            return values;
        }
        List<Object> result = new ArrayList<>();
        for (Object v : values) {
            result.add(valueForDisplay.apply(v));
        }
        return result;
    }

    public List<Object> fetchValuesInternal(SourceLookup lookup) {
        derivedFieldScript.setDocument(lookup.docId());
        derivedFieldScript.execute();
        return derivedFieldScript.getEmittedValues();
    }

    public List<IndexableField> getIndexableField(SourceLookup lookup, Function<Object, IndexableField> indexableFieldFunction) {
        List<Object> values = fetchValuesInternal(lookup);
        List<IndexableField> indexableFields = new ArrayList<>();
        for (Object v : values) {
            if (v != null) {
                indexableFields.add(indexableFieldFunction.apply(v));
            }
        }
        return indexableFields;
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        return null;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        try {
            derivedFieldScript = derivedFieldScriptFactory.newInstance(context);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return null;
    }
}
