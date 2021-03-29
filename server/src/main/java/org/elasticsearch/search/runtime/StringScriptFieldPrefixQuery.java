/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.lucene.search.AutomatonQueries;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.script.Script;

import java.util.List;
import java.util.Objects;

public class StringScriptFieldPrefixQuery extends AbstractStringScriptFieldQuery {
    private final String prefix;
    private final boolean caseInsensitive;

    public StringScriptFieldPrefixQuery(
        Script script,
        StringFieldScript.LeafFactory leafFactory,
        String fieldName,
        String prefix,
        boolean caseInsensitive
    ) {
        super(script, leafFactory, fieldName);
        this.prefix = Objects.requireNonNull(prefix);
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    protected boolean matches(List<String> values) {
        for (String value : values) {
            if (startsWith(value, prefix, caseInsensitive)) {
                return true;
            }
        }
        return false;
    }

    /**
     * <p>Check if a String starts with a specified prefix (optionally case insensitive).</p>
     *
     * @see java.lang.String#startsWith(String)
     * @param str  the String to check, may be null
     * @param prefix the prefix to find, may be null
     * @param ignoreCase inidicates whether the compare should ignore case
     *  (case insensitive) or not.
     * @return <code>true</code> if the String starts with the prefix or
     *  both <code>null</code>
     */
    private static boolean startsWith(String str, String prefix, boolean ignoreCase) {
        if (str == null || prefix == null) {
            return (str == null && prefix == null);
        }
        if (prefix.length() > str.length()) {
            return false;
        }
        return str.regionMatches(ignoreCase, 0, prefix, 0, prefix.length());
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName())) {
            visitor.consumeTermsMatching(this, fieldName(), () -> new ByteRunAutomaton(buildAutomaton(new BytesRef(prefix))));
        }
    }

    Automaton buildAutomaton(BytesRef prefix) {
        if (caseInsensitive) {
            return AutomatonQueries.caseInsensitivePrefix(prefix.utf8ToString());
        } else {
            return PrefixQuery.toAutomaton(prefix);
        }
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return prefix + "*";
        }
        return fieldName() + ":" + prefix + "*";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix, caseInsensitive);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        StringScriptFieldPrefixQuery other = (StringScriptFieldPrefixQuery) obj;
        return prefix.equals(other.prefix) && caseInsensitive == other.caseInsensitive;
    }

    String prefix() {
        return prefix;
    }

    boolean caseInsensitive() {
        return caseInsensitive;
    }
}
