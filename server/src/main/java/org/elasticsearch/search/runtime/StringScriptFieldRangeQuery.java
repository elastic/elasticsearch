/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.Script;

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
        if (lowerValue == null && includeLower == false) {
            throw new IllegalArgumentException("includeLower must be true when lowerValue is null (open ended)");
        }
        if (upperValue == null && includeUpper == false) {
            throw new IllegalArgumentException("includeUpper must be true when upperValue is null (open ended)");
        }
        this.lowerValue = lowerValue;
        this.upperValue = upperValue;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    @Override
    protected boolean matches(List<String> values) {
        for (String value : values) {
            boolean lowerOk = true;
            if (lowerValue != null) {
                int lct = lowerValue.compareTo(value);
                lowerOk = includeLower ? lct <= 0 : lct < 0;
            }
            if (lowerOk) {
                boolean upperOk = true;
                if (upperValue != null) {
                    int uct = upperValue.compareTo(value);
                    upperOk = includeUpper ? uct >= 0 : uct > 0;
                }
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
                    Automata.makeBinaryInterval(
                        lowerValue == null ? null : new BytesRef(lowerValue),
                        includeLower,
                        upperValue == null ? null : new BytesRef(upperValue),
                        includeUpper
                    )
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
        b.append(lowerValue == null ? "*" : lowerValue);
        b.append(" TO ");
        b.append(upperValue == null ? "*" : upperValue);
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
        return Objects.equals(lowerValue, other.lowerValue)
            && Objects.equals(upperValue, other.upperValue)
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
