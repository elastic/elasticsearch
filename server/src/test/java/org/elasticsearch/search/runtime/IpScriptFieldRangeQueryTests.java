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

public class IpScriptFieldRangeQueryTests extends AbstractIpScriptFieldQueryTestCase<IpScriptFieldRangeQuery> {
    @Override
    protected IpScriptFieldRangeQuery createTestInstance() {
        InetAddress lower = randomIp(randomBoolean());
        InetAddress upper = randomIp(randomBoolean());
        if (mustFlip(lower, upper)) {
            InetAddress tmp = lower;
            lower = upper;
            upper = tmp;
        }
        return new IpScriptFieldRangeQuery(randomScript(), leafFactory, randomAlphaOfLength(5), encode(lower), encode(upper));
    }

    @Override
    protected IpScriptFieldRangeQuery copy(IpScriptFieldRangeQuery orig) {
        return new IpScriptFieldRangeQuery(
            orig.script(),
            leafFactory,
            orig.fieldName(),
            encode(orig.lowerAddress()),
            encode(orig.upperAddress())
        );
    }

    @Override
    protected IpScriptFieldRangeQuery mutate(IpScriptFieldRangeQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        InetAddress lower = orig.lowerAddress();
        InetAddress upper = orig.upperAddress();
        switch (randomInt(3)) {
            case 0 -> script = randomValueOtherThan(script, this::randomScript);
            case 1 -> fieldName += "modified";
            case 2 -> lower = randomIp(randomBoolean());
            case 3 -> upper = randomIp(randomBoolean());
            default -> fail();
        }
        if (mustFlip(lower, upper)) {
            InetAddress tmp = lower;
            lower = upper;
            upper = tmp;
        }
        return new IpScriptFieldRangeQuery(script, leafFactory, fieldName, encode(lower), encode(upper));
    }

    @Override
    public void testMatches() {
        // Try with ipv4
        BytesRef min = encode(InetAddresses.forString("192.168.0.1"));
        BytesRef max = encode(InetAddresses.forString("200.255.255.255"));
        IpScriptFieldRangeQuery query = new IpScriptFieldRangeQuery(randomScript(), leafFactory, "test", min, max);
        assertTrue(query.matches(new BytesRef[] { min }, 1));
        assertTrue(query.matches(new BytesRef[] { max }, 1));
        assertTrue(query.matches(new BytesRef[] { encode(InetAddresses.forString("200.255.255.0")) }, 1));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("127.0.0.1")) }, 0));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("127.0.0.1")) }, 1));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("241.0.0.0")) }, 1));
        assertTrue(query.matches(new BytesRef[] { min, encode(InetAddresses.forString("241.0.0.0")) }, 2));
        assertTrue(query.matches(new BytesRef[] { encode(InetAddresses.forString("241.0.0.0")), min }, 2));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("241.0.0.0")), min }, 1));

        // Now ipv6
        min = encode(InetAddresses.forString("ff00::"));
        max = encode(InetAddresses.forString("fff0:ffff:ffff:ffff:ffff:ffff:ffff:ffff"));
        query = new IpScriptFieldRangeQuery(randomScript(), leafFactory, "test", min, max);
        assertTrue(query.matches(new BytesRef[] { min }, 1));
        assertTrue(query.matches(new BytesRef[] { max }, 1));
        assertTrue(query.matches(new BytesRef[] { encode(InetAddresses.forString("ff00::1")) }, 1));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("fa00::")) }, 1));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("127.0.0.1")) }, 1));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("127.0.0.1")) }, 0));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("241.0.0.0")) }, 1));
        assertTrue(query.matches(new BytesRef[] { min, encode(InetAddresses.forString("fa00::")) }, 2));
        assertTrue(query.matches(new BytesRef[] { encode(InetAddresses.forString("fa00::")), min }, 2));
        assertFalse(query.matches(new BytesRef[] { encode(InetAddresses.forString("fa00::")), min }, 1));

        // Finally, try with an ipv6 range that contains ipv4
        min = encode(InetAddresses.forString("::fff:0:0"));
        max = encode(InetAddresses.forString("::ffff:ffff:ffff"));
        query = new IpScriptFieldRangeQuery(randomScript(), leafFactory, "test", min, max);
        assertTrue(query.matches(new BytesRef[] { min }, 1));
        assertTrue(query.matches(new BytesRef[] { max }, 1));
        assertTrue(query.matches(new BytesRef[] { encode(InetAddresses.forString("127.0.0.1")) }, 1));
        assertTrue(query.matches(new BytesRef[] { encode(InetAddresses.forString("241.0.0.0")) }, 1));
    }

    @Override
    protected void assertToString(IpScriptFieldRangeQuery query) {
        assertThat(
            query.toString(query.fieldName()),
            equalTo(
                "[" + InetAddresses.toAddrString(query.lowerAddress()) + " TO " + InetAddresses.toAddrString(query.upperAddress()) + "]"
            )
        );
    }

    private boolean mustFlip(InetAddress minCandidate, InetAddress maxCandidate) {
        return encode(minCandidate).compareTo(encode(maxCandidate)) > 0;
    }
}
