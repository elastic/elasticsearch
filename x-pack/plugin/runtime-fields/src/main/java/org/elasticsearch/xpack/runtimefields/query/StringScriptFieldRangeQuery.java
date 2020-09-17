/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.StringFieldScript;

import java.util.List;
import java.util.Objects;

public class StringScriptFieldRangeQuery extends AbstractStringScriptFieldQuery {
    private final String lowerValue;
    private final String upperValue;
    private final boolean includeLower;
    private final boolean includeUpper;

    public StringScriptFieldRangeQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        String lowerValue,
        String upperValue,
        boolean includeLower,
        boolean includeUpper
    ) {
        super(script, leafFactory, fieldName);
        this.lowerValue = Objects.requireNonNull(lowerValue);
        this.upperValue = Objects.requireNonNull(upperValue);
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
        assert lowerValue.compareTo(upperValue) <= 0;
    }

    @Override
    protected boolean matches(List<String> values) {
        for (String value : values) {
            int lct = lowerValue.compareTo(value);
            boolean lowerOk = includeLower ? lct <= 0 : lct < 0;
            if (lowerOk) {
                int uct = upperValue.compareTo(value);
                boolean upperOk = includeUpper ? uct >= 0 : uct > 0;
                if (upperOk) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName())) {
            visitor.consumeTermsMatching(
                this,
                fieldName(),
                () -> new ByteRunAutomaton(
                    Automata.makeBinaryInterval(new BytesRef(lowerValue), includeLower, new BytesRef(upperValue), includeUpper)
                )
            );
        }
    }

    @Override
    public final String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (false == fieldName().contentEquals(field)) {
            b.append(fieldName()).append(':');
        }
        b.append(includeLower ? '[' : '{');
        b.append(lowerValue).append(" TO ").append(upperValue);
        b.append(includeUpper ? ']' : '}');
        return b.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lowerValue, upperValue, includeLower, includeUpper);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldRangeQuery other = (StringScriptFieldRangeQuery) obj;
        return lowerValue.equals(other.lowerValue)
            && upperValue.equals(other.upperValue)
            && includeLower == other.includeLower
            && includeUpper == other.includeUpper;
    }

    String lowerValue() {
        return lowerValue;
    }

    String upperValue() {
        return upperValue;
    }

    boolean includeLower() {
        return includeLower;
    }

    boolean includeUpper() {
        return includeUpper;
    }
}
