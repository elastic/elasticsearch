/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.transforms;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class PainlessDomainSplitIT extends ESRestTestCase {

    private static final Map<String, String> EXCLUDED;

    static {
        EXCLUDED = new HashMap<>();
        EXCLUDED.put("city.yokohama.jp", "i");
        EXCLUDED.put("teledata.mz", "i");
        EXCLUDED.put("city.kobe.jp", "i");
        EXCLUDED.put("city.sapporo.jp", "i");
        EXCLUDED.put("city.kawasaki.jp", "i");
        EXCLUDED.put("city.nagoya.jp", "i");
        EXCLUDED.put("www.ck", "i");
        EXCLUDED.put("city.sendai.jp", "i");
        EXCLUDED.put("city.kitakyushu.jp", "i");
    }

    private static final Map<String, String> UNDER;

    static {
        UNDER = new HashMap<>();
        UNDER.put("bd", "i");
        UNDER.put("np", "i");
        UNDER.put("jm", "i");
        UNDER.put("fj", "i");
        UNDER.put("fk", "i");
        UNDER.put("ye", "i");
        UNDER.put("sch.uk", "i");
        UNDER.put("bn", "i");
        UNDER.put("kitakyushu.jp", "i");
        UNDER.put("kobe.jp", "i");
        UNDER.put("ke", "i");
        UNDER.put("sapporo.jp", "i");
        UNDER.put("kh", "i");
        UNDER.put("mm", "i");
        UNDER.put("il", "i");
        UNDER.put("yokohama.jp", "i");
        UNDER.put("ck", "i");
        UNDER.put("nagoya.jp", "i");
        UNDER.put("sendai.jp", "i");
        UNDER.put("kw", "i");
        UNDER.put("er", "i");
        UNDER.put("mz", "i");
        UNDER.put("platform.sh", "p");
        UNDER.put("gu", "i");
        UNDER.put("nom.br", "i");
        UNDER.put("zm", "i");
        UNDER.put("pg", "i");
        UNDER.put("ni", "i");
        UNDER.put("kawasaki.jp", "i");
        UNDER.put("zw", "i");
    }

    public static final Map<String, Object> params;

    static {
        params = new HashMap<>();

        ResourceBundle resource = ResourceBundle.getBundle("org/elasticsearch/xpack/ml/transforms/exact", Locale.getDefault());
        Enumeration keys = resource.getKeys();
        Map<String, String> exact = new HashMap<>(2048);
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            String value = resource.getString(key);
            exact.put(key, value);
        }
        params.put("excluded", EXCLUDED);
        params.put("under", UNDER);
        params.put("exact", exact);
    }

    static String domainSplitFunction;

    static {

        try {
            domainSplitFunction = StreamsUtils.copyToStringFromClasspath("/org/elasticsearch/xpack/ml/transforms/script.painless")
                    .replace("\n", "");
        } catch (Exception e) {
            domainSplitFunction = "";
            fail(e.toString());
        }
    }

    static class TestConfiguration {
        public String subDomainExpected;
        public String domainExpected;
        public String hostName;

        TestConfiguration(String subDomainExpected, String domainExpected, String hostName) {
            this.subDomainExpected = subDomainExpected;
            this.domainExpected = domainExpected;
            this.hostName = hostName;
        }
    }

    public static final ArrayList<TestConfiguration> tests;

    static {
        tests = new ArrayList<>();

        tests.add(new TestConfiguration("", "", ""));
        tests.add(new TestConfiguration("", "", "."));

        // Test cases from https://github.com/john-kurkowski/tldextract/tree/master/tldextract/tests

        tests.add(new TestConfiguration("www", "google.com", "www.google.com"));
        tests.add(new TestConfiguration("www.maps", "google.co.uk", "www.maps.google.co.uk"));
        tests.add(new TestConfiguration("www", "theregister.co.uk", "www.theregister.co.uk"));
        tests.add(new TestConfiguration("", "gmail.com", "gmail.com"));
        tests.add(new TestConfiguration("media.forums", "theregister.co.uk", "media.forums.theregister.co.uk"));
        tests.add(new TestConfiguration("www", "www.com", "www.www.com"));
        tests.add(new TestConfiguration("", "www.com", "www.com"));
        tests.add(new TestConfiguration("", "internalunlikelyhostname", "internalunlikelyhostname"));
        tests.add(new TestConfiguration("internalunlikelyhostname", "bizarre", "internalunlikelyhostname.bizarre"));
        tests.add(new TestConfiguration("", "internalunlikelyhostname.info", "internalunlikelyhostname.info"));  // .info is a valid TLD
        tests.add(new TestConfiguration("internalunlikelyhostname", "information", "internalunlikelyhostname.information"));
        tests.add(new TestConfiguration("", "216.22.0.192", "216.22.0.192"));
        tests.add(new TestConfiguration("", "::1", "::1"));
        tests.add(new TestConfiguration("", "FE80:0000:0000:0000:0202:B3FF:FE1E:8329", "FE80:0000:0000:0000:0202:B3FF:FE1E:8329"));
        tests.add(new TestConfiguration("216.22", "project.coop", "216.22.project.coop"));
        tests.add(new TestConfiguration("www", "xn--h1alffa9f.xn--p1ai", "www.xn--h1alffa9f.xn--p1ai"));
        tests.add(new TestConfiguration("", "", ""));
        tests.add(new TestConfiguration("www", "parliament.uk", "www.parliament.uk"));
        tests.add(new TestConfiguration("www", "parliament.co.uk", "www.parliament.co.uk"));
        tests.add(new TestConfiguration("www.a", "cgs.act.edu.au", "www.a.cgs.act.edu.au"));
        tests.add(new TestConfiguration("www", "google.com.au", "www.google.com.au"));
        tests.add(new TestConfiguration("www", "metp.net.cn", "www.metp.net.cn"));
        tests.add(new TestConfiguration("www", "waiterrant.blogspot.com", "www.waiterrant.blogspot.com"));
        tests.add(new TestConfiguration("", "kittens.blogspot.co.uk", "kittens.blogspot.co.uk"));
        tests.add(new TestConfiguration("", "prelert.s3.amazonaws.com", "prelert.s3.amazonaws.com"));
        tests.add(new TestConfiguration("daves_bucket", "prelert.s3.amazonaws.com", "daves_bucket.prelert.s3.amazonaws.com"));
        tests.add(new TestConfiguration("example", "example", "example.example"));
        tests.add(new TestConfiguration("b.example", "example", "b.example.example"));
        tests.add(new TestConfiguration("a.b.example", "example", "a.b.example.example"));
        tests.add(new TestConfiguration("example", "local", "example.local"));
        tests.add(new TestConfiguration("b.example", "local", "b.example.local"));
        tests.add(new TestConfiguration("a.b.example", "local", "a.b.example.local"));
        tests.add(new TestConfiguration("r192494180984795-1-1041782-channel-live.ums", "ustream.tv", "r192494180984795-1-1041782-cha" +
                "nnel-live.ums.ustream.tv"));
        tests.add(new TestConfiguration("192.168.62.9", "prelert.com", "192.168.62.9.prelert.com"));

        // These are not a valid DNS names
        tests.add(new TestConfiguration("kerberos.http.192.168", "62.222", "kerberos.http.192.168.62.222"));
        //tests.add(new TestConfiguration("192.168", "62.9\143\127", "192.168.62.9\143\127"));

        // no part of the DNS name can be longer than 63 octets
        /*
        String dnsLongerThan254Chars = "davesbucketdavesbucketdavesbucketdavesbucketdavesbucketdaves.bucketdavesbucketdavesbuc" +
                "ketdavesbucketdavesbucketdaves.bucketdavesbucketdavesbucketdavesbucketdavesbucket.davesbucketdavesbucketdaves" +
                "bucketdavesbucket.davesbucketdavesbucket.prelert.s3.amazonaws.com";
        String hrd = "prelert.s3.amazonaws.com";
        tests.add(new TestConfiguration(dnsLongerThan254Chars.substring(0, dnsLongerThan254Chars.length() - (hrd.length() + 1)),
                hrd, dnsLongerThan254Chars));
        */

        // [Zach] This breaks the script's JSON encoding, skipping for now
        //String bad = "0u1aof\209\1945\188hI4\236\197\205J\244\188\247\223\190F\2135\229gVE7\230i\215\231\205Qzay\225UJ\192
        // pw\216\231\204\194\216\193QV4g\196\207Whpvx.fVxl\194BjA\245kbYk\211XG\235\198\218B\252\219\225S\197\217I\2538n\229
        // \244\213\252\215Ly\226NW\242\248\244Q\220\245\221c\207\189\205Hxq5\224\240.\189Jt4\243\245t\244\198\199p\210\1987
        // r\2050L\239sR0M\190w\238\223\234L\226\2242D\233\210\206\195h\199\206tA\214J\192C\224\191b\188\201\251\198M\244h
        // \206.\198\242l\2114\191JBU\198h\207\215w\243\228R\1924\242\208\191CV\208p\197gDW\198P\217\195X\191Fp\196\197J\193
        // \245\2070\196zH\197\243\253g\239.adz.beacon.base.net";
        //hrd = "base.net";
        //tests.add(new TestConfiguration(bad.substring(0, bad.length() - (hrd.length() + 1)), hrd, bad));


        tests.add(new TestConfiguration("_example", "local", "_example.local"));
        tests.add(new TestConfiguration("www._maps", "google.co.uk", "www._maps.google.co.uk"));
        tests.add(new TestConfiguration("-forum", "theregister.co.uk", "-forum.theregister.co.uk"));
        tests.add(new TestConfiguration("www._yourmp", "parliament.uk", "www._yourmp.parliament.uk"));
        tests.add(new TestConfiguration("www.-a", "cgs.act.edu.au", "www.-a.cgs.act.edu.au"));
        tests.add(new TestConfiguration("", "-foundation.org", "-foundation.org"));
        tests.add(new TestConfiguration("www", "-foundation.org", "www.-foundation.org"));
        tests.add(new TestConfiguration("", "_nfsv4idmapdomain", "_nfsv4idmapdomain"));
        tests.add(new TestConfiguration("_nfsv4idmapdomain", "prelert.com", "_nfsv4idmapdomain.prelert.com"));

        // checkHighestRegisteredDomain() tests
        tests.add(new TestConfiguration(null, "example.com", "example.COM"));
        tests.add(new TestConfiguration(null, "example.com", "WwW.example.COM"));

        // TLD with only 1 rule.
        tests.add(new TestConfiguration(null, "domain.biz", "domain.biz" ));
        tests.add(new TestConfiguration(null, "domain.biz", "b.domain.biz"));
        tests.add(new TestConfiguration(null, "domain.biz", "a.b.domain.biz"));

        // TLD with some 2-level rules.
        tests.add(new TestConfiguration(null, "example.com", "example.com"));
        tests.add(new TestConfiguration(null, "example.com", "b.example.com"));
        tests.add(new TestConfiguration(null, "example.com", "a.b.example.com"));
        tests.add(new TestConfiguration(null, "example.uk.com", "example.uk.com"));
        tests.add(new TestConfiguration(null, "example.uk.com", "b.example.uk.com"));
        tests.add(new TestConfiguration(null, "example.uk.com", "a.b.example.uk.com"));
        tests.add(new TestConfiguration(null, "test.ac", "test.ac"));
        tests.add(new TestConfiguration(null, "c.gov.cy", "c.gov.cy"));
        tests.add(new TestConfiguration(null, "c.gov.cy", "b.c.gov.cy"));
        tests.add(new TestConfiguration(null, "c.gov.cy", "a.b.c.gov.cy"));

        // more complex TLD
        tests.add(new TestConfiguration(null, "test.jp", "test.jp"));
        tests.add(new TestConfiguration(null, "test.jp", "www.test.jp"));
        tests.add(new TestConfiguration(null, "test.ac.jp", "test.ac.jp"));
        tests.add(new TestConfiguration(null, "test.ac.jp", "www.test.ac.jp"));
        tests.add(new TestConfiguration(null, "test.kyoto.jp", "test.kyoto.jp"));
        tests.add(new TestConfiguration(null, "b.ide.kyoto.jp", "b.ide.kyoto.jp"));
        tests.add(new TestConfiguration(null, "b.ide.kyoto.jp", "a.b.ide.kyoto.jp"));
        //tests.add(new TestConfiguration(null, "b.c.kobe.jp", "b.c.kobe.jp"));
        //tests.add(new TestConfiguration(null, "b.c.kobe.jp", "a.b.c.kobe.jp"));
        tests.add(new TestConfiguration(null, "city.kobe.jp", "city.kobe.jp"));
        tests.add(new TestConfiguration(null, "city.kobe.jp", "www.city.kobe.jp"));
        tests.add(new TestConfiguration(null, "test.us", "test.us"));
        tests.add(new TestConfiguration(null, "test.us", "www.test.us"));
        tests.add(new TestConfiguration(null, "test.ak.us", "test.ak.us"));
        tests.add(new TestConfiguration(null, "test.ak.us", "www.test.ak.us"));
        tests.add(new TestConfiguration(null, "test.k12.ak.us", "test.k12.ak.us"));
        tests.add(new TestConfiguration(null, "test.k12.ak.us", "www.test.k12.ak.us"));
        //tests.add(new TestConfiguration(null, "食狮.com.cn", "食狮.com.cn"));
        //tests.add(new TestConfiguration(null, "食狮.公司.cn", "食狮.公司.cn"));
        //tests.add(new TestConfiguration(null, "食狮.公司.cn", "www.食狮.公司.cn"));
        //tests.add(new TestConfiguration(null, "shishi.公司.cn", "shishi.公司.cn"));
        //tests.add(new TestConfiguration(null, "食狮.中国", "食狮.中国"));
        //tests.add(new TestConfiguration(null, "食狮.中国", "www.食狮.中国"));
        //tests.add(new TestConfiguration(null, "shishi.中国", "shishi.中国"));

        tests.add(new TestConfiguration(null, "xn--85x722f.com.cn", "xn--85x722f.com.cn"));
        tests.add(new TestConfiguration(null,  "xn--85x722f.xn--55qx5d.cn", "xn--85x722f.xn--55qx5d.cn"));
        tests.add(new TestConfiguration(null, "xn--85x722f.xn--55qx5d.cn", "www.xn--85x722f.xn--55qx5d.cn"));
        tests.add(new TestConfiguration(null, "shishi.xn--55qx5d.cn", "shishi.xn--55qx5d.cn"));
        tests.add(new TestConfiguration(null, "xn--85x722f.xn--fiqs8s", "xn--85x722f.xn--fiqs8s"));
        tests.add(new TestConfiguration(null, "xn--85x722f.xn--fiqs8s", "www.xn--85x722f.xn--fiqs8s"));
        tests.add(new TestConfiguration(null, "shishi.xn--fiqs8s","shishi.xn--fiqs8s"));
    }

    private void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    private void createIndex(String name, Settings settings) throws IOException {
        assertOK(client().performRequest("PUT", name, Collections.emptyMap(),
                new StringEntity("{ \"settings\": " + Strings.toString(settings) + " }")));
    }

    public void testBasics() throws Exception {

        Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0);

        createIndex("painless", settings.build());
        client().performRequest("PUT", "painless/test/1", Collections.emptyMap(), new StringEntity("{\"test\": \"test\"}"));
        client().performRequest("POST", "painless/_refresh");

        Pattern pattern = Pattern.compile("domain_split\":\\[(.*?),(.*?)\\]");

        for (TestConfiguration testConfig : tests) {
            params.put("host", testConfig.hostName);
            String mapAsJson = new ObjectMapper().writeValueAsString(params);

            StringEntity body = new StringEntity("{\n" +
                    "    \"query\" : {\n" +
                    "        \"match_all\": {}\n" +
                    "    },\n" +
                    "    \"script_fields\" : {\n" +
                    "        \"domain_split\" : {\n" +
                    "            \"script\" : {\n" +
                    "                \"lang\": \"painless\",\n" +
                    "                \"inline\": \"" + domainSplitFunction + " return domainSplit(params['host'], params); \",\n" +
                    "                \"params\": " + mapAsJson + "\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}");

            Response response = client().performRequest("GET", "painless/test/_search", Collections.emptyMap(), body);
            String responseBody = EntityUtils.toString(response.getEntity());
            Matcher m = pattern.matcher(responseBody);

            String actualSubDomain = "";
            String actualDomain = "";
            if (m.find()) {
                actualSubDomain = m.group(1).replace("\"", "");
                actualDomain = m.group(2).replace("\"", "");
            }

            String expectedTotal = "[" + testConfig.subDomainExpected + "," + testConfig.domainExpected + "]";
            String actualTotal = "[" + actualSubDomain + "," + actualDomain + "]";

            // domainSplit() tests had subdomain, testHighestRegisteredDomainCases() do not
            if (testConfig.subDomainExpected != null) {
                assertThat("Expected subdomain [" + testConfig.subDomainExpected + "] but found [" + actualSubDomain + "]. Actual "
                        + actualTotal + " vs Expected " + expectedTotal, actualSubDomain, equalTo(testConfig.subDomainExpected));
            }

            assertThat("Expected domain [" + testConfig.domainExpected + "] but found [" + actualDomain + "].  Actual "
                    + actualTotal + " vs Expected " + expectedTotal, actualDomain, equalTo(testConfig.domainExpected));
        }

    }
}
