/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query for the {@code key\0value} slots an automaton accepts, against a field stored in the
 * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull KeyedArrayOrderInlineNull}
 * binary doc-values format used by flattened fields in columnar mode. It is the automaton counterpart of
 * {@link KeyedArrayOrderInlineNullTermQuery}: it reads each slot whole, key included, which the scan of the plain
 * array-order format does not.
 */
public final class KeyedArrayOrderInlineNullAutomatonQuery extends AbstractBinaryDocValuesQuery {

    private final ByteRunAutomaton runAutomaton;
    private final String description;

    /**
     * @param automaton   the automaton over {@code key\0value} that a value of the key must match
     * @param description how the query prints the pattern it matches
     */
    public KeyedArrayOrderInlineNullAutomatonQuery(String fieldName, Automaton automaton, String description) {
        this(fieldName, new ByteRunAutomaton(automaton), description);
    }

    private KeyedArrayOrderInlineNullAutomatonQuery(String fieldName, ByteRunAutomaton runAutomaton, String description) {
        super(fieldName, value -> runAutomaton.run(value.bytes, value.offset, value.length), BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL);
        this.runAutomaton = runAutomaton;
        this.description = Objects.requireNonNull(description);
    }

    @Override
    protected DocIdSetIterator getDocIdSetIterator(LeafReaderContext context, float matchCost) throws IOException {
        BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return null;
        }
        NumericDocValues counts = context.reader().getNumericDocValues(fieldName + COUNT_FIELD_SUFFIX);
        assert counts != null : "KeyedArrayOrderInlineNull always writes a companion count field";
        return keyedInlineNullIterator(values, counts, matcher, matchCost);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.consumeTermsMatching(this, fieldName, () -> runAutomaton);
        }
    }

    @Override
    protected float matchCost() {
        return 1000f;
    }

    @Override
    public String toString(String field) {
        return "KeyedArrayOrderInlineNullAutomatonQuery(fieldName=" + fieldName + "," + description + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        KeyedArrayOrderInlineNullAutomatonQuery that = (KeyedArrayOrderInlineNullAutomatonQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(description, that.description)
            && runAutomaton.equals(that.runAutomaton);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, description, runAutomaton);
    }
}
