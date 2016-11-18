/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.transforms;

import org.elasticsearch.test.ESTestCase;

// TODO Reimplement
public class HighestRegisteredDomainTests extends ESTestCase {
    // private void checkHighestRegisteredDomain(String fullName, String
    // registeredNameExpected)
    // {
    // InternetDomainName effectiveTLD = InternetDomainName.from(fullName);
    //
    // effectiveTLD = effectiveTLD.topPrivateDomain();
    // assertTrue(effectiveTLD.isTopPrivateDomain());
    // String registeredName = effectiveTLD.toString();
    //
    // assertEquals(registeredNameExpected, registeredName);
    // }
    //
    // private void checkIsPublicSuffix(String suffix)
    // {
    // InternetDomainName effectiveTLD = InternetDomainName.from(suffix);
    // assertTrue(effectiveTLD.isPublicSuffix());
    // }
    //
    // private void testDomainSplit(String subDomainExpected,
    // String domainExpected, String hostName)
    // {
    // HighestRegisteredDomain.DomainSplit split =
    // HighestRegisteredDomain.lookup(hostName);
    //
    // assertEquals(subDomainExpected, split.getSubDomain());
    // assertEquals(domainExpected, split.getHighestRegisteredDomain());
    // }
    //
    // @Test
    // public void testDomainSplit()
    // {
    // testDomainSplit("", "", "");
    // testDomainSplit("", "", ".");
    //
    // // Test cases from
    // https://github.com/john-kurkowski/tldextract/tree/master/tldextract/tests
    // testDomainSplit("www", "google.com", "www.google.com");
    // testDomainSplit("www.maps", "google.co.uk", "www.maps.google.co.uk");
    // testDomainSplit("www", "theregister.co.uk", "www.theregister.co.uk");
    // testDomainSplit("", "gmail.com", "gmail.com");
    // testDomainSplit("media.forums", "theregister.co.uk",
    // "media.forums.theregister.co.uk");
    // testDomainSplit("www", "www.com", "www.www.com");
    // testDomainSplit("", "www.com", "www.com");
    // testDomainSplit("", "internalunlikelyhostname",
    // "internalunlikelyhostname");
    // testDomainSplit("internalunlikelyhostname", "bizarre",
    // "internalunlikelyhostname.bizarre");
    // testDomainSplit("", "internalunlikelyhostname.info",
    // "internalunlikelyhostname.info"); // .info is a valid TLD
    // testDomainSplit("internalunlikelyhostname", "information",
    // "internalunlikelyhostname.information");
    // testDomainSplit("", "216.22.0.192", "216.22.0.192");
    // testDomainSplit("", "::1", "::1");
    // testDomainSplit("", "FE80:0000:0000:0000:0202:B3FF:FE1E:8329",
    // "FE80:0000:0000:0000:0202:B3FF:FE1E:8329");
    // testDomainSplit("216.22", "project.coop", "216.22.project.coop");
    // testDomainSplit("www", "xn--h1alffa9f.xn--p1ai",
    // "www.xn--h1alffa9f.xn--p1ai");
    // testDomainSplit("", "", "");
    // testDomainSplit("www", "parliament.uk", "www.parliament.uk");
    // testDomainSplit("www", "parliament.co.uk", "www.parliament.co.uk");
    // testDomainSplit("www.a", "cgs.act.edu.au", "www.a.cgs.act.edu.au");
    // testDomainSplit("www", "google.com.au", "www.google.com.au");
    // testDomainSplit("www", "metp.net.cn", "www.metp.net.cn");
    // testDomainSplit("www", "waiterrant.blogspot.com",
    // "www.waiterrant.blogspot.com");
    //
    // testDomainSplit("", "kittens.blogspot.co.uk", "kittens.blogspot.co.uk");
    // testDomainSplit("", "prelert.s3.amazonaws.com",
    // "prelert.s3.amazonaws.com");
    // testDomainSplit("daves_bucket", "prelert.s3.amazonaws.com",
    // "daves_bucket.prelert.s3.amazonaws.com");
    //
    // testDomainSplit("example", "example", "example.example");
    // testDomainSplit("b.example", "example", "b.example.example");
    // testDomainSplit("a.b.example", "example", "a.b.example.example");
    //
    // testDomainSplit("example", "local", "example.local");
    // testDomainSplit("b.example", "local", "b.example.local");
    // testDomainSplit("a.b.example", "local", "a.b.example.local");
    //
    // testDomainSplit("r192494180984795-1-1041782-channel-live.ums",
    // "ustream.tv", "r192494180984795-1-1041782-channel-live.ums.ustream.tv");
    //
    // testDomainSplit("192.168.62.9", "prelert.com",
    // "192.168.62.9.prelert.com");
    //
    // // These are not a valid DNS names
    // testDomainSplit("kerberos.http.192.168", "62.222",
    // "kerberos.http.192.168.62.222");
    // testDomainSplit("192.168", "62.9\143\127", "192.168.62.9\143\127");
    // }
    //
    // @Test
    // public void testTooLongDnsName()
    // {
    // // no part of the DNS name can be longer than 63 octets
    // String dnsLongerThan254Chars =
    // "davesbucketdavesbucketdavesbucketdavesbucketdavesbucketdaves.bucketdavesbucketdavesbucketdavesbucketdavesbucketdaves.bucketdav
    // esbucketdavesbucketdavesbucketdavesbucket.davesbucketdavesbucketdavesbucketdavesbucket.davesbucketdavesbucket.prelert.s3.a
    // mazonaws.com";
    // String hrd = "prelert.s3.amazonaws.com";
    // testDomainSplit(dnsLongerThan254Chars.substring(0,
    // dnsLongerThan254Chars.length() - (hrd.length() + 1)),
    // hrd, dnsLongerThan254Chars);
    //
    // // this one needs sanitising
    // dnsLongerThan254Chars =
    // "_davesbucketdavesbucketdavesbucketdavesbucket-davesbucketdaves.-bucketdavesbucketdavesbucketdavesbucketdavesbucketdaves.bucket
    // davesbucketdavesbucketdavesbucketdavesbucket.davesbucketdavesbucketdavesbucketdavesbucket.davesbucketdavesbucket.prelert.s3.ama
    // zonaws.com";
    // hrd = "prelert.s3.amazonaws.com";
    // testDomainSplit(dnsLongerThan254Chars.substring(0,
    // dnsLongerThan254Chars.length() - (hrd.length() + 1)),
    // hrd, dnsLongerThan254Chars);
    //
    // String bad =
    // "0u1aof\209\1945\188hI4\236\197\205J\244\188\247\223\190F\2135\229gVE7\230i\215\231\205Qzay\225UJ\192pw\216\231\204\194\216\
    // 193QV4g\196\207Whpvx.fVxl\194BjA\245kbYk\211XG\235\198\218B\252\219\225S\197\217I\2538n\229\244\213\252\215Ly\226NW\242\248\
    // 244Q\220\245\221c\207\189\205Hxq5\224\240.\189Jt4\243\245t\244\198\199p\210\1987r\2050L\239sR0M\190w\238\223\234L\226\2242D\233
    // \210\206\195h\199\206tA\214J\192C\224\191b\188\201\251\198M\244h\206.\198\242l\2114\191JBU\198h\207\215w\243\228R\1924\242\208\19
    // 1CV\208p\197gDW\198P\217\195X\191Fp\196\197J\193\245\2070\196zH\197\243\253g\239.adz.beacon.base.net";
    // hrd = "base.net";
    // testDomainSplit(bad.substring(0, bad.length() - (hrd.length() +1)), hrd,
    // bad);
    // }
    //
    // @Test
    // public void testDomainSplit_SanitisedDomains()
    // {
    // testDomainSplit("_example", "local", "_example.local");
    // testDomainSplit("www._maps", "google.co.uk", "www._maps.google.co.uk");
    // testDomainSplit("-forum", "theregister.co.uk",
    // "-forum.theregister.co.uk");
    //
    // testDomainSplit("www._yourmp", "parliament.uk",
    // "www._yourmp.parliament.uk");
    // testDomainSplit("www.-a", "cgs.act.edu.au", "www.-a.cgs.act.edu.au");
    //
    // testDomainSplit("", "-foundation.org", "-foundation.org");
    // testDomainSplit("www", "-foundation.org", "www.-foundation.org");
    // testDomainSplit("", "_nfsv4idmapdomain", "_nfsv4idmapdomain");
    // testDomainSplit("_nfsv4idmapdomain", "prelert.com",
    // "_nfsv4idmapdomain.prelert.com");
    //
    // testDomainSplit("lb._dns-sd._udp.0.123.168", "192.in-addr.arpa",
    // "lb._dns-sd._udp.0.123.168.192.in-addr.arpa");
    // testDomainSplit("_kerberos._http.192.168", "62.222",
    // "_kerberos._http.192.168.62.222");
    // }
    //
    // @Test
    // public void testHighestRegisteredDomainCases()
    // {
    // // Any copyright is dedicated to the Public Domain.
    // // http://creativecommons.org/publicdomain/zero/1.0/
    //
    // // Domain parts starting with _ aren't valid
    // assertFalse(InternetDomainName.isValid("_nfsv4idmapdomain.prelert.com"));
    //
    // // Mixed case.
    // checkIsPublicSuffix("COM");
    // checkHighestRegisteredDomain("example.COM", "example.com");
    // checkHighestRegisteredDomain("WwW.example.COM", "example.com");
    //
    // // These pass steve's test but fail here. Example isn't a valid
    // (declared, not active) TLD
    //// checkIsPublicSuffix("example");
    //// checkTopLevelDomain("example.example", "example.example");
    //// checkTopLevelDomain("b.example.example", "example.example");
    //// checkTopLevelDomain("a.b.example.example", "example.example");
    //
    // // Listed, but non-Internet, TLD.
    // // checkIsPublicSuffix("local"); // These pass Steve's tests but not
    // public suffix here
    // //checkIsPublicSuffix("example.local", "");
    // //checkIsPublicSuffix("b.example.local", "");
    // //checkIsPublicSuffix("a.b.example.local", "");
    //
    // // TLD with only 1 rule.
    // checkIsPublicSuffix("biz");
    // checkHighestRegisteredDomain("domain.biz", "domain.biz");
    // checkHighestRegisteredDomain("b.domain.biz", "domain.biz");
    // checkHighestRegisteredDomain("a.b.domain.biz", "domain.biz");
    // // TLD with some 2-level rules.
    // // checkPublicSuffix("com", "");
    // checkHighestRegisteredDomain("example.com", "example.com");
    // checkHighestRegisteredDomain("b.example.com", "example.com");
    // checkHighestRegisteredDomain("a.b.example.com", "example.com");
    // checkIsPublicSuffix("uk.com");
    // checkHighestRegisteredDomain("example.uk.com", "example.uk.com");
    // checkHighestRegisteredDomain("b.example.uk.com", "example.uk.com");
    // checkHighestRegisteredDomain("a.b.example.uk.com", "example.uk.com");
    // checkHighestRegisteredDomain("test.ac", "test.ac");
    // // TLD with only 1 (wildcard) rule.
    //
    // // cy passes Steve's test but is not considered a valid TLD here
    // // gov.cy is.
    // checkIsPublicSuffix("gov.cy");
    // checkHighestRegisteredDomain("c.gov.cy", "c.gov.cy"); // changed to pass
    // test - inserted .gov, .net
    // checkHighestRegisteredDomain("b.c.net.cy", "c.net.cy");
    // checkHighestRegisteredDomain("a.b.c.net.cy", "c.net.cy");
    //
    // // More complex TLD.
    // checkIsPublicSuffix("jp"); // jp is valid because you can have any 2nd
    // level domain
    // checkIsPublicSuffix("ac.jp");
    // checkIsPublicSuffix("kyoto.jp");
    // checkIsPublicSuffix("c.kobe.jp");
    // checkIsPublicSuffix("ide.kyoto.jp");
    // checkHighestRegisteredDomain("test.jp", "test.jp");
    // checkHighestRegisteredDomain("www.test.jp", "test.jp");
    // checkHighestRegisteredDomain("test.ac.jp", "test.ac.jp");
    // checkHighestRegisteredDomain("www.test.ac.jp", "test.ac.jp");
    // checkHighestRegisteredDomain("test.kyoto.jp", "test.kyoto.jp");
    // checkHighestRegisteredDomain("b.ide.kyoto.jp", "b.ide.kyoto.jp");
    // checkHighestRegisteredDomain("a.b.ide.kyoto.jp", "b.ide.kyoto.jp");
    // checkHighestRegisteredDomain("b.c.kobe.jp", "b.c.kobe.jp");
    // checkHighestRegisteredDomain("a.b.c.kobe.jp", "b.c.kobe.jp");
    // checkHighestRegisteredDomain("city.kobe.jp", "city.kobe.jp");
    // checkHighestRegisteredDomain("www.city.kobe.jp", "city.kobe.jp");
    //
    //
    // // TLD with a wildcard rule and exceptions.
    //// checkIsPublicSuffix("ck"); // Passes Steve's test but is not considered
    // a valid TLD here
    //// checkIsPublicSuffix("test.ck");
    //// checkTopLevelDomain("b.test.ck", "b.test.ck");
    //// checkTopLevelDomain("a.b.test.ck", "b.test.ck");
    //// checkTopLevelDomain("www.ck", "www.ck");
    //// checkTopLevelDomain("www.www.ck", "www.ck");
    //
    // // US K12.
    // checkIsPublicSuffix("us");
    // checkIsPublicSuffix("ak.us");
    // checkIsPublicSuffix("k12.ak.us");
    // checkHighestRegisteredDomain("test.us", "test.us");
    // checkHighestRegisteredDomain("www.test.us", "test.us");
    // checkHighestRegisteredDomain("test.ak.us", "test.ak.us");
    // checkHighestRegisteredDomain("www.test.ak.us", "test.ak.us");
    // checkHighestRegisteredDomain("test.k12.ak.us", "test.k12.ak.us");
    // checkHighestRegisteredDomain("www.test.k12.ak.us", "test.k12.ak.us");
    //
    // // IDN labels.
    // checkIsPublicSuffix("公司.cn");
    // checkIsPublicSuffix("中国");
    // checkHighestRegisteredDomain("食狮.com.cn", "食狮.com.cn");
    // checkHighestRegisteredDomain("食狮.公司.cn", "食狮.公司.cn");
    // checkHighestRegisteredDomain("www.食狮.公司.cn", "食狮.公司.cn");
    // checkHighestRegisteredDomain("shishi.公司.cn", "shishi.公司.cn");
    // checkHighestRegisteredDomain("食狮.中国", "食狮.中国");
    // checkHighestRegisteredDomain("www.食狮.中国", "食狮.中国");
    // checkHighestRegisteredDomain("shishi.中国", "shishi.中国");
    //
    // // Same as above, but punycoded.
    // checkIsPublicSuffix("xn--55qx5d.cn");
    // checkIsPublicSuffix("xn--fiqs8s");
    // checkHighestRegisteredDomain("xn--85x722f.com.cn", "xn--85x722f.com.cn");
    // checkHighestRegisteredDomain("xn--85x722f.xn--55qx5d.cn",
    // "xn--85x722f.xn--55qx5d.cn");
    // checkHighestRegisteredDomain("www.xn--85x722f.xn--55qx5d.cn",
    // "xn--85x722f.xn--55qx5d.cn");
    // checkHighestRegisteredDomain("shishi.xn--55qx5d.cn",
    // "shishi.xn--55qx5d.cn");
    // checkHighestRegisteredDomain("xn--85x722f.xn--fiqs8s",
    // "xn--85x722f.xn--fiqs8s");
    // checkHighestRegisteredDomain("www.xn--85x722f.xn--fiqs8s",
    // "xn--85x722f.xn--fiqs8s");
    // checkHighestRegisteredDomain("shishi.xn--fiqs8s", "shishi.xn--fiqs8s");
    // }
    //
    // @Test
    // public void testSanitiseDomainName()
    // {
    // String ok_domain = "nfsv4idmapdomain.prelert.com";
    // assertTrue(InternetDomainName.isValid(ok_domain));
    // assertTrue(HighestRegisteredDomain.sanitiseDomainName(ok_domain) ==
    // ok_domain);
    // ok_domain = "nfsv4idmapdomain\u3002prelert\uFF0Ecom";
    // assertTrue(InternetDomainName.isValid(ok_domain));
    // assertTrue(HighestRegisteredDomain.sanitiseDomainName(ok_domain) ==
    // ok_domain);
    // ok_domain = "www.test.ac\uFF61jp";
    // assertTrue(InternetDomainName.isValid(ok_domain));
    // assertTrue(HighestRegisteredDomain.sanitiseDomainName(ok_domain) ==
    // ok_domain);
    // ok_domain = "xn--85x722f.com.cn";
    // assertTrue(InternetDomainName.isValid(ok_domain));
    // assertTrue(HighestRegisteredDomain.sanitiseDomainName(ok_domain) ==
    // ok_domain);
    // ok_domain = "x_n--85x722f.com.cn";
    // assertTrue(InternetDomainName.isValid(ok_domain));
    // assertTrue(HighestRegisteredDomain.sanitiseDomainName(ok_domain) ==
    // ok_domain);
    // ok_domain = "食狮.com.cn";
    // assertTrue(InternetDomainName.isValid(ok_domain));
    // assertTrue(HighestRegisteredDomain.sanitiseDomainName(ok_domain) ==
    // ok_domain);
    //
    // String bad_domain = "_nfsv4idmapdomain.prelert.com";
    // assertFalse(InternetDomainName.isValid(bad_domain));
    // String sanitisedDomain =
    // HighestRegisteredDomain.sanitiseDomainName(bad_domain);
    // assertTrue(sanitisedDomain != ok_domain);
    // assertEquals("p_nfsv4idmapdomain.pprelert.com", sanitisedDomain);
    // assertEquals(bad_domain,
    // HighestRegisteredDomain.desanitise(sanitisedDomain));
    //
    // bad_domain = "_www.test.ac\uFF61jp";
    // assertFalse(InternetDomainName.isValid(bad_domain));
    // sanitisedDomain = HighestRegisteredDomain.sanitiseDomainName(bad_domain);
    // assertTrue(sanitisedDomain != ok_domain);
    // assertEquals(HighestRegisteredDomain.replaceDots("p_www.test.ac\uFF61jp"),
    // sanitisedDomain);
    // assertEquals(HighestRegisteredDomain.replaceDots(bad_domain),
    // HighestRegisteredDomain.desanitise(sanitisedDomain));
    //
    // bad_domain = "_xn--85x722f.com.cn";
    // assertFalse(InternetDomainName.isValid(bad_domain));
    // sanitisedDomain = HighestRegisteredDomain.sanitiseDomainName(bad_domain);
    // assertTrue(sanitisedDomain != ok_domain);
    // assertEquals("p_xn--85x722f.com.cn", sanitisedDomain);
    // assertEquals(bad_domain,
    // HighestRegisteredDomain.desanitise(sanitisedDomain));
    //
    // bad_domain = "-foundation.org";
    // assertFalse(InternetDomainName.isValid(bad_domain));
    // sanitisedDomain = HighestRegisteredDomain.sanitiseDomainName(bad_domain);
    // assertTrue(sanitisedDomain != ok_domain);
    // assertEquals("p-foundation.org", sanitisedDomain);
    // assertEquals(bad_domain,
    // HighestRegisteredDomain.desanitise(sanitisedDomain));
    // }
    //
    // /**
    // * Get sub domain only
    // * @throws TransformException
    // */
    // @Test
    // public void testTransform_SingleOutput() throws TransformException
    // {
    // List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0,
    // 2));
    // List<TransformIndex> writeIndexes = createIndexArray(new
    // TransformIndex(2, 0));
    //
    // HighestRegisteredDomain transform = new HighestRegisteredDomain(
    // readIndexes, writeIndexes, mock(Logger.class));
    //
    // String [] input = {"", "", "www.test.ac.jp"};
    // String [] scratch = {};
    // String [] output = new String [2];
    // String [][] readWriteArea = {input, scratch, output};
    //
    // transform.transform(readWriteArea);
    // assertEquals("www", output[0]);
    // assertNull(output[1]);
    //
    // input[2] = "a.b.domain.biz";
    // transform.transform(readWriteArea);
    // assertEquals("a.b", output[0]);
    // assertNull(output[1]);
    // }
    //
    //
    //
    // @Test
    // public void testTransform_AllOutputs() throws TransformException
    // {
    // List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(0,
    // 2));
    // List<TransformIndex> writeIndexes = createIndexArray(new
    // TransformIndex(2, 0), new TransformIndex(2, 1));
    //
    // HighestRegisteredDomain transform = new HighestRegisteredDomain(
    // readIndexes, writeIndexes, mock(Logger.class));
    //
    //
    // String [] input = {"", "", "www.test.ac.jp"};
    // String [] scratch = {};
    // String [] output = new String [2];
    // String [][] readWriteArea = {input, scratch, output};
    //
    // transform.transform(readWriteArea);
    // assertEquals("www", output[0]);
    // assertEquals("test.ac.jp", output[1]);
    //
    // input[2] = "a.b.domain.biz";
    // transform.transform(readWriteArea);
    // assertEquals("a.b", output[0]);
    // assertEquals("domain.biz", output[1]);
    // }
    //
    // @Test
    // public void testTransformTrimWhiteSpace() throws TransformException
    // {
    // List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(1,
    // 2));
    // List<TransformIndex> writeIndexes = createIndexArray(new
    // TransformIndex(2, 0), new TransformIndex(2, 1));
    //
    // HighestRegisteredDomain transform = new HighestRegisteredDomain(
    // readIndexes, writeIndexes, mock(Logger.class));
    //
    // String [] input = {};
    // String [] scratch = {"", "", " time.apple.com "};
    // String [] output = new String [2];
    // String [][] readWriteArea = {input, scratch, output};
    //
    // transform.transform(readWriteArea);
    // assertEquals("time", output[0]);
    // assertEquals("apple.com", output[1]);
    // }
    //
    // @Test
    // public void testTransform_WriteToScratch() throws TransformException
    // {
    // List<TransformIndex> readIndexes = createIndexArray(new TransformIndex(1,
    // 2));
    // List<TransformIndex> writeIndexes = createIndexArray(new
    // TransformIndex(2, 0), new TransformIndex(2, 1));
    //
    // HighestRegisteredDomain transform = new HighestRegisteredDomain(
    // readIndexes, writeIndexes, mock(Logger.class));
    //
    // String [] input = {};
    // String [] scratch = {"", "", " time.apple.com "};
    // String [] output = new String [2];
    // String [][] readWriteArea = {input, scratch, output};
    //
    // assertEquals(TransformResult.OK, transform.transform(readWriteArea));
    // assertEquals("time", output[0]);
    // assertEquals("apple.com", output[1]);
    // }
}
