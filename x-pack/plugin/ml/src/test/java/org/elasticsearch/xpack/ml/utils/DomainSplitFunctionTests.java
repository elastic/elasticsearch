/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class DomainSplitFunctionTests extends ESTestCase {

    public void testDomainSplit() {
        // Test cases from https://github.com/john-kurkowski/tldextract/tree/master/tldextract/tests
        assertDomainSplit("www", "google.com", "www.google.com");
        assertDomainSplit("www.maps", "google.co.uk", "www.maps.google.co.uk");
        assertDomainSplit("www", "theregister.co.uk", "www.theregister.co.uk");
        assertDomainSplit("", "gmail.com", "gmail.com");
        assertDomainSplit("media.forums", "theregister.co.uk", "media.forums.theregister.co.uk");
        assertDomainSplit("www", "www.com", "www.www.com");
        assertDomainSplit("", "www.com", "www.com");
        assertDomainSplit("", "internalunlikelyhostname", "internalunlikelyhostname");
        assertDomainSplit("internalunlikelyhostname", "bizarre", "internalunlikelyhostname.bizarre");
        assertDomainSplit("", "internalunlikelyhostname.info", "internalunlikelyhostname.info");  // .info is a valid TLD
        assertDomainSplit("internalunlikelyhostname", "information", "internalunlikelyhostname.information");
        assertDomainSplit("", "216.22.0.192", "216.22.0.192");
        assertDomainSplit("", "::1", "::1");
        assertDomainSplit("", "FE80:0000:0000:0000:0202:B3FF:FE1E:8329", "FE80:0000:0000:0000:0202:B3FF:FE1E:8329");
        assertDomainSplit("216.22", "project.coop", "216.22.project.coop");
        assertDomainSplit("www", "xn--h1alffa9f.xn--p1ai", "www.xn--h1alffa9f.xn--p1ai");
        assertDomainSplit("", "", "");
        assertDomainSplit("www", "parliament.uk", "www.parliament.uk");
        assertDomainSplit("www", "parliament.co.uk", "www.parliament.co.uk");
        assertDomainSplit("www.a", "cgs.act.edu.au", "www.a.cgs.act.edu.au");
        assertDomainSplit("www", "google.com.au", "www.google.com.au");
        assertDomainSplit("www", "metp.net.cn", "www.metp.net.cn");
        assertDomainSplit("www", "waiterrant.blogspot.com", "www.waiterrant.blogspot.com");
        assertDomainSplit("", "kittens.blogspot.co.uk", "kittens.blogspot.co.uk");
        assertDomainSplit("example", "example", "example.example");
        assertDomainSplit("b.example", "example", "b.example.example");
        assertDomainSplit("a.b.example", "example", "a.b.example.example");
        assertDomainSplit("example", "local", "example.local");
        assertDomainSplit("b.example", "local", "b.example.local");
        assertDomainSplit("a.b.example", "local", "a.b.example.local");
        assertDomainSplit(
            "r192494180984795-1-1041782-channel-live.ums",
            "ustream.tv",
            "r192494180984795-1-1041782-channel-live.ums.ustream.tv"
        );
    }

    private void assertDomainSplit(String expectedSubDomain, String expectedDomain, String hostName) {
        List<String> split = DomainSplitFunction.domainSplit(hostName);
        assertEquals(expectedSubDomain, split.get(0));
        assertEquals(expectedDomain, split.get(1));
    }
}
