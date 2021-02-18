/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.IpFieldScript;

import java.net.InetAddress;
import java.util.Objects;

public class IpScriptFieldTermQuery extends AbstractIpScriptFieldQuery {
    private final BytesRef term;

    public IpScriptFieldTermQuery(Script script, IpFieldScript.LeafFactory leafFactory, String fieldName, BytesRef term) {
        super(script, leafFactory, fieldName);
        this.term = term;
    }

    @Override
    protected boolean matches(BytesRef[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (term.bytesEquals(values[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return InetAddresses.toAddrString(address());
        }
        return fieldName() + ":" + InetAddresses.toAddrString(address());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), term);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        IpScriptFieldTermQuery other = (IpScriptFieldTermQuery) obj;
        return term.bytesEquals(other.term);
    }

    InetAddress address() {
        return decode(term);
    }
}
