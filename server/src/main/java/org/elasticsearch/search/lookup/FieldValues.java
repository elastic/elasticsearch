/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;

import java.util.function.Consumer;

/**
 * Represents values for a given document
 */
public interface FieldValues<T> {

    /**
     * Loads the values for the given document and passes them to the consumer
     * @param lookup    a search lookup to access values from
     * @param ctx       the LeafReaderContext containing the document
     * @param doc       the docid
     * @param consumer  called with each document value
     */
    void valuesForDoc(SearchLookup lookup, LeafReaderContext ctx, int doc, Consumer<T> consumer);
}
