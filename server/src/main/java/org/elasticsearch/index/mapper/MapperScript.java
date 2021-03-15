/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * A script executed to populate values on a FieldMapper
 * @param <T> the type of object indexed by the mapper
 */
public abstract class MapperScript<T> implements ToXContentObject {

    private final Script script;

    protected MapperScript(Script script) {
        this.script = script;
    }

    /**
     * Execute the mapper script and emit its results
     * @param lookup    a SearchLookup for use by the script
     * @param ctx       a LeafReaderContext for use by the script
     * @param doc       the document to retrieve values for
     * @param emitter   a consumer to receive the retrieved values
     */
    public abstract void executeAndEmit(SearchLookup lookup, LeafReaderContext ctx, int doc, Consumer<T> emitter);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapperScript<?> that = (MapperScript<?>) o;
        return Objects.equals(script, that.script);
    }

    @Override
    public int hashCode() {
        return Objects.hash(script);
    }

    @Override
    public String toString() {
        return script.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return script.toXContent(builder, params);
    }
}
