/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorFormat;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * A {@link ValueFetcher} for {@code dense_vector} fields that reads the vector from doc values rather than
 * from {@code _source}. Used with synthetic source, where {@code _source} would have to be rebuilt from the
 * same doc values anyway.
 */
class DenseVectorDocValuesValueFetcher implements ValueFetcher {
    private final DenseVectorSyntheticFieldLoader loader;
    private final ElementType elementType;
    private final VectorFormat format;

    private SourceLoader.SyntheticFieldLoader.DocValuesLoader docValuesLoader;

    DenseVectorDocValuesValueFetcher(DenseVectorSyntheticFieldLoader loader, ElementType elementType, VectorFormat format) {
        this.loader = loader;
        this.elementType = elementType;
        this.format = format;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        try {
            docValuesLoader = loader.docValuesLoader(context.reader(), null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        if (docValuesLoader == null || docValuesLoader.advanceToDoc(doc) == false || loader.hasValue() == false) {
            return List.of();
        }
        return switch (format) {
            case ARRAY -> loader.vectorAsList(true);
            case BINARY -> List.of(DenseVectorSourceValueFetcher.encodeBase64(loader.vectorAsList(false), elementType));
        };
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return StoredFieldsSpec.NO_REQUIREMENTS;
    }
}
