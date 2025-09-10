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
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;

public class SyntheticVectorsPatchFieldLoader implements SourceLoader.SyntheticVectorsLoader {
    private final SourceLoader.SyntheticFieldLoader syntheticLoader;
    private final CheckedSupplier<Object, IOException> copyObject;

    public SyntheticVectorsPatchFieldLoader(
        SourceLoader.SyntheticFieldLoader syntheticLoader,
        CheckedSupplier<Object, IOException> copyObject
    ) {
        this.syntheticLoader = syntheticLoader;
        this.copyObject = copyObject;
    }

    public SourceLoader.SyntheticVectorsLoader.Leaf leaf(LeafReaderContext context) throws IOException {
        var dvLoader = syntheticLoader.docValuesLoader(context.reader(), null);
        return (doc, acc) -> {
            if (dvLoader == null) {
                return;
            }
            if (dvLoader.advanceToDoc(doc) && syntheticLoader.hasValue()) {
                acc.add(new SourceLoader.LeafSyntheticVectorPath(syntheticLoader.fieldName(), copyObject.get()));
            }
        };
    }
}
