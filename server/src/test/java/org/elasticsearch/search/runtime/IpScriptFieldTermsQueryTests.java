/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.script.Script;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class IpScriptFieldTermsQueryTests extends AbstractIpScriptFieldQueryTestCase<IpScriptFieldTermsQuery> {
    @Override
    protected IpScriptFieldTermsQuery createTestInstance() {
        return createTestInstance(between(1, 100));
    }

    private IpScriptFieldTermsQuery createTestInstance(int size) {
        BytesRefHash terms = new BytesRefHash(size, BigArrays.NON_RECYCLING_INSTANCE);
        while (terms.size() < size) {
            terms.add(new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))));
        }
        return new IpScriptFieldTermsQuery(randomScript(), leafFactory, randomAlphaOfLength(5), terms);
    }

    @Override
    protected IpScriptFieldTermsQuery copy(IpScriptFieldTermsQuery orig) {
        return new IpScriptFieldTermsQuery(orig.script(), leafFactory, orig.fieldName(), copyTerms(orig.terms()));
    }

    private BytesRefHash copyTerms(BytesRefHash terms) {
        BytesRefHash copy = new BytesRefHash(terms.size(), BigArrays.NON_RECYCLING_INSTANCE);
        BytesRef spare = new BytesRef();
        for (long i = 0; i < terms.size(); i++) {
            terms.get(i, spare);
            assertEquals(i, copy.add(spare));
        }
        return copy;
    }

    @Override
    protected IpScriptFieldTermsQuery mutate(IpScriptFieldTermsQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        BytesRefHash terms = copyTerms(orig.terms());
        switch (randomInt(2)) {
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                long size = terms.size() + 1;
                while (terms.size() < size) {
                    terms.add(new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))));
                }
                break;
            default:
                fail();
        }
        return new IpScriptFieldTermsQuery(script, leafFactory, fieldName, terms);
    }

    @Override
    public void testMatches() {
        BytesRef ip1 = encode(InetAddresses.forString("192.168.0.1"));
        BytesRef ip2 = encode(InetAddresses.forString("192.168.0.2"));
        BytesRef notIp = encode(InetAddresses.forString("192.168.0.3"));

        BytesRefHash terms = new BytesRefHash(2, BigArrays.NON_RECYCLING_INSTANCE);
        terms.add(ip1);
        terms.add(ip2);
        IpScriptFieldTermsQuery query = new IpScriptFieldTermsQuery(randomScript(), leafFactory, "test", terms);
        assertTrue(query.matches(new BytesRef[] { ip1 }, 1));
        assertTrue(query.matches(new BytesRef[] { ip2 }, 1));
        assertTrue(query.matches(new BytesRef[] { ip1, notIp }, 2));
        assertTrue(query.matches(new BytesRef[] { notIp, ip1 }, 2));
        assertFalse(query.matches(new BytesRef[] { notIp }, 1));
        assertFalse(query.matches(new BytesRef[] { notIp, ip1 }, 1));
    }

    @Override
    protected void assertToString(IpScriptFieldTermsQuery query) {
        if (query.toString(query.fieldName()).contains("...")) {
            assertBigToString(query);
        } else {
            assertLittleToString(query);
        }
    }

    private void assertBigToString(IpScriptFieldTermsQuery query) {
        String toString = query.toString(query.fieldName());
        BytesRef spare = new BytesRef();
        assertThat(toString, startsWith("["));
        query.terms().get(0, spare);
        assertThat(
            toString,
            containsString(InetAddresses.toAddrString(InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(spare)))))
        );
        query.terms().get(query.terms().size() - 1, spare);
        assertThat(
            toString,
            not(containsString(InetAddresses.toAddrString(InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(spare))))))
        );
        assertThat(toString, endsWith("...]"));
    }

    private void assertLittleToString(IpScriptFieldTermsQuery query) {
        String toString = query.toString(query.fieldName());
        BytesRef spare = new BytesRef();
        assertThat(toString, startsWith("["));
        for (long i = 0; i < query.terms().size(); i++) {
            query.terms().get(i, spare);
            assertThat(
                toString,
                containsString(InetAddresses.toAddrString(InetAddressPoint.decode(BytesReference.toBytes(new BytesArray(spare)))))
            );
        }
        assertThat(toString, endsWith("]"));
    }

    public void testBigToString() {
        assertBigToString(createTestInstance(1000));
    }

    public void testLittleToString() {
        assertLittleToString(createTestInstance(5));
    }
}
