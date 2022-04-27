/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.Script;

import java.util.Objects;

public class IpScriptFieldTermsQuery extends AbstractIpScriptFieldQuery {
    private final BytesRefHash terms;

    public IpScriptFieldTermsQuery(Script script, IpFieldScript.LeafFactory leafFactory, String fieldName, BytesRefHash terms) {
        super(script, leafFactory, fieldName);
        this.terms = terms;
    }

    @Override
    protected boolean matches(BytesRef[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (terms.find(values[i]) >= 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (false == fieldName().contentEquals(field)) {
            b.append(fieldName()).append(":");
        }
        b.append("[");
        BytesRef spare = new BytesRef();
        long i = 0;
        while (i < terms.size() && b.length() < 5000) {
            if (i != 0) {
                b.append(", ");
            }
            b.append(InetAddresses.toAddrString(decode(terms.get(i++, spare))));
        }
        if (i < terms.size()) {
            b.append("...");
        }
        return b.append("]").toString();
    }

    @Override
    public int hashCode() {
        long hash = 0;
        BytesRef spare = new BytesRef();
        for (long i = 0; i < terms.size(); i++) {
            hash = 31 * hash + terms.get(i, spare).hashCode();
        }
        return Objects.hash(super.hashCode(), hash);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        IpScriptFieldTermsQuery other = (IpScriptFieldTermsQuery) obj;
        if (terms.size() != other.terms.size()) {
            return false;
        }
        BytesRef mySpare = new BytesRef();
        BytesRef otherSpare = new BytesRef();
        for (long i = 0; i < terms.size(); i++) {
            terms.get(i, mySpare);
            other.terms.get(i, otherSpare);
            if (false == mySpare.bytesEquals(otherSpare)) {
                return false;
            }
        }
        return true;
    }

    BytesRefHash terms() {
        return terms;
    }
}
