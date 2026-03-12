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
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;
import java.util.function.Supplier;

public class SyntheticVectorsPatchFieldLoader<T extends SourceLoader.SyntheticFieldLoader> implements SourceLoader.SyntheticVectorsLoader {
    private final Supplier<T> syntheticLoaderSupplier;
    private final CheckedFunction<T, Object, IOException> copyObject;

    public SyntheticVectorsPatchFieldLoader(Supplier<T> syntheticLoaderSupplier, CheckedFunction<T, Object, IOException> copyObject) {
        this.syntheticLoaderSupplier = syntheticLoaderSupplier;
        this.copyObject = copyObject;
    }

    public SourceLoader.SyntheticVectorsLoader.Leaf leaf(LeafReaderContext context) throws IOException {
        var syntheticLoader = syntheticLoaderSupplier.get();
        var dvLoader = syntheticLoader.docValuesLoader(context.reader(), null);
        return (doc, acc) -> {
            if (dvLoader == null) {
                return;
            }
            if (dvLoader.advanceToDoc(doc) && syntheticLoader.hasValue()) {
                acc.add(new SourceLoader.LeafSyntheticVectorPath(syntheticLoader.fieldName(), copyObject.apply(syntheticLoader)));
            }
        };
    }
}
