/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.TermsSetQueryScript;

import java.io.IOException;
import java.util.Objects;

/** A {@link LongValuesSource} backed by a {@link TermsSetQueryScript}, used for {@code terms_set}'s {@code minimum_should_match_script}. */
public final class ScriptLongValueSource extends LongValuesSource {

    private final Script script;
    private final TermsSetQueryScript.LeafFactory leafFactory;
    private final Runnable cancellationCheck;

    public ScriptLongValueSource(Script script, TermsSetQueryScript.LeafFactory leafFactory, Runnable cancellationCheck) {
        this.script = script;
        this.leafFactory = leafFactory;
        this.cancellationCheck = cancellationCheck;
    }

    @Override
    public LongValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
        TermsSetQueryScript script = leafFactory.newInstance(ctx);
        script._setCancellationCheck(cancellationCheck);
        return new LongValues() {
            @Override
            public long longValue() {
                return script.runAsLong();
            }

            @Override
            public boolean advanceExact(int doc) {
                script.setDocument(doc);
                return script.execute() != null;
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + script.hashCode();
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ScriptLongValueSource that = (ScriptLongValueSource) obj;
        return Objects.equals(script, that.script);
    }

    @Override
    public String toString() {
        return "script(" + script.toString() + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        // TODO: Change this to true when we can assume that scripts are pure functions
        // ie. the return value is always the same given the same conditions and may not
        // depend on the current timestamp, other documents, etc.
        return false;
    }

    @Override
    public LongValuesSource rewrite(IndexSearcher searcher) throws IOException {
        return this;
    }
}
