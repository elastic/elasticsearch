/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.lucene.search.AutomatonQueries;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A query that matches binary doc values for documents whose field value starts with the given prefix.
 * The equivalent of {@link org.elasticsearch.search.runtime.StringScriptFieldPrefixQuery}, but without the scripting overhead and
 * just for binary doc values.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class ScanningBinaryDocValuesPrefixQuery extends AbstractBinaryDocValuesQuery {

    private final String prefix;
    private final boolean caseInsensitive;

    public ScanningBinaryDocValuesPrefixQuery(String fieldName, String prefix, boolean caseInsensitive, boolean arrayOrderInlineNull) {
        super(fieldName, buildMatcher(Objects.requireNonNull(prefix), caseInsensitive), arrayOrderInlineNull);
        this.prefix = prefix;
        this.caseInsensitive = caseInsensitive;
    }

    private static Predicate<BytesRef> buildMatcher(String prefix, boolean caseInsensitive) {
        if (caseInsensitive) {
            ByteRunAutomaton automaton = new ByteRunAutomaton(AutomatonQueries.caseInsensitivePrefix(prefix));
            return value -> automaton.run(value.bytes, value.offset, value.length);
        }
        BytesRef prefixBytes = new BytesRef(prefix);
        return value -> value.length >= prefixBytes.length
            && Arrays.equals(
                value.bytes,
                value.offset,
                value.offset + prefixBytes.length,
                prefixBytes.bytes,
                prefixBytes.offset,
                prefixBytes.offset + prefixBytes.length
            );
    }

    @Override
    protected float matchCost() {
        return caseInsensitive ? 1000f : 10f;
    }

    @Override
    public String toString(String field) {
        return "ScanningBinaryDocValuesPrefixQuery(fieldName="
            + fieldName
            + ",prefix="
            + prefix
            + ",caseInsensitive="
            + caseInsensitive
            + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScanningBinaryDocValuesPrefixQuery that = (ScanningBinaryDocValuesPrefixQuery) o;
        return caseInsensitive == that.caseInsensitive && Objects.equals(fieldName, that.fieldName) && Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, prefix, caseInsensitive);
    }
}
