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
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.Script;

import java.net.InetAddress;
import java.util.Objects;

public class IpScriptFieldRangeQuery extends AbstractIpScriptFieldQuery {
    private final BytesRef lower;
    private final BytesRef upper;

    public IpScriptFieldRangeQuery(Script script, IpFieldScript.LeafFactory leafFactory, String fieldName, BytesRef lower, BytesRef upper) {
        super(script, leafFactory, fieldName);
        this.lower = lower;
        this.upper = upper;
        assert this.lower.compareTo(this.upper) <= 0;
    }

    @Override
    protected boolean matches(BytesRef[] values, int count) {
        for (int i = 0; i < count; i++) {
            if (lower.compareTo(values[i]) <= 0 && upper.compareTo(values[i]) >= 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final String toString(String field) {
        StringBuilder b = new StringBuilder();
        if (false == fieldName().contentEquals(field)) {
            b.append(fieldName()).append(':');
        }
        b.append('[')
            .append(InetAddresses.toAddrString(lowerAddress()))
            .append(" TO ")
            .append(InetAddresses.toAddrString(upperAddress()))
            .append(']');
        return b.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), lower, upper);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        IpScriptFieldRangeQuery other = (IpScriptFieldRangeQuery) obj;
        return lower.bytesEquals(other.lower) && upper.bytesEquals(other.upper);
    }

    InetAddress lowerAddress() {
        return decode(lower);
    }

    InetAddress upperAddress() {
        return decode(upper);
    }
}
