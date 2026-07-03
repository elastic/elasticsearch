/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

/**
 * Base class for binary doc values queries that match documents using a {@link ByteRunAutomaton}.
 * Stores the automaton so it can be surfaced to {@link QueryVisitor#consumeTermsMatching} for
 * highlighter support, and provides a shared {@link #matchCost()} estimate and {@link #visit} implementation.
 */
abstract class AbstractBinaryDocValuesAutomatonQuery extends AbstractBinaryDocValuesQuery {

    final ByteRunAutomaton automaton;

    AbstractBinaryDocValuesAutomatonQuery(String fieldName, ByteRunAutomaton automaton, boolean arrayOrderInlineNull) {
        super(fieldName, value -> automaton.run(value.bytes, value.offset, value.length), arrayOrderInlineNull);
        this.automaton = automaton;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.consumeTermsMatching(this, fieldName, () -> automaton);
        }
    }

    @Override
    protected float matchCost() {
        return 1000f;
    }
}
