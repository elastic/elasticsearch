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
import org.elasticsearch.script.Script;

import java.net.InetAddress;

import static org.hamcrest.Matchers.equalTo;

public class IpScriptFieldTermQueryTests extends AbstractIpScriptFieldQueryTestCase<IpScriptFieldTermQuery> {
    @Override
    protected IpScriptFieldTermQuery createTestInstance() {
        return new IpScriptFieldTermQuery(randomScript(), leafFactory, randomAlphaOfLength(5), encode(randomIp(randomBoolean())));
    }

    @Override
    protected IpScriptFieldTermQuery copy(IpScriptFieldTermQuery orig) {
        return new IpScriptFieldTermQuery(orig.script(), leafFactory, orig.fieldName(), encode(orig.address()));
    }

    @Override
    protected IpScriptFieldTermQuery mutate(IpScriptFieldTermQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        InetAddress term = orig.address();
        switch (randomInt(2)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                term = randomValueOtherThan(term, () -> randomIp(randomBoolean()));
                break;
            default:
                fail();
        }
        return new IpScriptFieldTermQuery(script, leafFactory, fieldName, encode(term));
    }

    @Override
    public void testMatches() {
        BytesRef ip = encode(randomIp(randomBoolean()));
        BytesRef notIpRef = randomValueOtherThan(ip, () -> encode(randomIp(randomBoolean())));
        IpScriptFieldTermQuery query = new IpScriptFieldTermQuery(randomScript(), leafFactory, "test", ip);
        assertTrue(query.matches(new BytesRef[] { ip }, 1));            // Match because value matches
        assertFalse(query.matches(new BytesRef[] { notIpRef }, 1));     // No match because wrong value
        assertFalse(query.matches(new BytesRef[] { notIpRef, ip }, 1)); // No match because value after count of values
        assertTrue(query.matches(new BytesRef[] { notIpRef, ip }, 2));  // Match because one value matches
    }

    @Override
    protected void assertToString(IpScriptFieldTermQuery query) {
        assertThat(query.toString(query.fieldName()), equalTo(InetAddresses.toAddrString(query.address())));
    }
}
