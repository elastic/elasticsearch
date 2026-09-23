/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Equivalent to {@link org.apache.lucene.search.AutomatonQuery}, but over an automaton the caller already
 * compiled. Lucene's query compiles its own in the constructor, which forces one transition table per clause
 * even when a pattern is expanded over many fields; taking a {@link SharedAutomaton} lets those clauses reuse
 * a single table.
 */
public final class SharedAutomatonQuery extends MultiTermQuery implements Accountable {

    private static final long BASE_RAM_BYTES = RamUsageEstimator.shallowSizeOfInstance(SharedAutomatonQuery.class);

    private final Term term;
    private final SharedAutomaton shared;
    private final Function<String, String> description;

    /**
     * @param description renders the query given the field {@link #toString(String)} was called with. Callers own their
     *                    own format, since the queries this replaces do not agree on one: see {@link #fieldPrefixed}.
     */
    public SharedAutomatonQuery(Term term, SharedAutomaton shared, Function<String, String> description, RewriteMethod rewriteMethod) {
        super(term.field(), rewriteMethod);
        this.term = term;
        this.shared = shared;
        this.description = description;
    }

    /**
     * Renders {@code description}, qualified by the field unless {@code toString} was already called with it. Matches
     * how {@link org.elasticsearch.index.query.AutomatonQueryWithDescription} prints.
     */
    public static Function<String, String> fieldPrefixed(Term term, String description) {
        return field -> term.field().equals(field) ? description : term.field() + ":" + description;
    }

    @Override
    protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        return shared.compiled().getTermsEnum(terms);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            shared.compiled().visit(visitor, this, field);
        }
    }

    public Automaton getAutomaton() {
        return shared.automaton();
    }

    public SharedAutomaton getSharedAutomaton() {
        return shared;
    }

    /**
     * The full graph size, as {@link Accountable} defines it, even when the automaton is shared with other clauses.
     * Consumers that hold a single clause, such as the query cache, would under-count the memory they are keeping
     * alive if this reported only the unshared part, so it must not be narrowed to {@link #unsharedRamBytesUsed()}.
     * Summing it over sibling clauses over-counts the shared automaton instead, which is why request accounting
     * charges the automaton once where it is built and adds only {@link #unsharedRamBytesUsed()} per clause.
     */
    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES + term.ramBytesUsed() + shared.ramBytesUsed();
    }

    /** What this clause costs on top of the automaton it shares. */
    public long unsharedRamBytesUsed() {
        return BASE_RAM_BYTES + term.ramBytesUsed();
    }

    @Override
    public String toString(String field) {
        return description.apply(field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term, shared.compiled());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return super.equals(obj)
            && obj instanceof SharedAutomatonQuery other
            && term.equals(other.term)
            && shared.compiled().equals(other.shared.compiled());
    }
}
