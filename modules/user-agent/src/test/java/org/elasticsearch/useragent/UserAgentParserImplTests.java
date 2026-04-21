/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.useragent.api.Details;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@SuppressWarnings("DataFlowIssue")
public class UserAgentParserImplTests extends ESTestCase {

    private static UserAgentParserImpl parser;

    @BeforeClass
    public static void setupParser() {
        InputStream regexStream = UserAgentPlugin.class.getResourceAsStream("/regexes.yml");
        InputStream deviceTypeRegexStream = UserAgentPlugin.class.getResourceAsStream("/device_type_regexes.yml");
        assertNotNull(regexStream);
        assertNotNull(deviceTypeRegexStream);
        parser = new UserAgentParserImpl("_default", regexStream, deviceTypeRegexStream, new UserAgentCache(1000));
    }

    public void testChromeMacOs() {
        Details details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.149 Safari/537.36",
            true
        );

        assertThat(details.name(), equalTo("Chrome"));
        assertThat(details.version(), equalTo("33.0.1750.149"));

        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("Mac OS X"));
        assertThat(details.os().version(), equalTo("10.9.2"));
        assertThat(details.osFull(), equalTo("Mac OS X 10.9.2"));

        assertThat(details.device(), notNullValue());
        assertThat(details.device().name(), equalTo("Mac"));

        assertThat(details.deviceType(), equalTo("Desktop"));
    }

    public void testChromeWindows() {
        Details details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
            true
        );

        assertThat(details.name(), equalTo("Chrome"));
        assertThat(details.version(), equalTo("87.0.4280.141"));

        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("Windows"));
        assertThat(details.os().version(), equalTo("10"));
        assertThat(details.osFull(), equalTo("Windows 10"));

        assertThat(details.device(), nullValue());

        assertThat(details.deviceType(), equalTo("Desktop"));
    }

    public void testFirefox() {
        Details details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
            true
        );

        assertThat(details.name(), equalTo("Firefox"));
        assertThat(details.version(), equalTo("128.0"));

        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("Windows"));

        assertThat(details.deviceType(), equalTo("Desktop"));
    }

    public void testAndroidMobile() {
        Details details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10+ "
                + "(KHTML, like Gecko) Version/3.0.4 Mobile Safari/523.12.2",
            true
        );

        assertThat(details.name(), equalTo("Android"));
        assertThat(details.version(), equalTo("3.0"));

        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("Android"));
        assertThat(details.os().version(), equalTo("3.0"));

        assertThat(details.device(), notNullValue());
        assertThat(details.device().name(), equalTo("Motorola Xoom"));

        assertThat(details.deviceType(), equalTo("Phone"));
    }

    public void testIPadTablet() {
        Details details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                + "Version/12.1 Mobile/15E148 Safari/604.1",
            true
        );

        assertThat(details.name(), equalTo("Mobile Safari"));
        assertThat(details.version(), equalTo("12.1"));

        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("iOS"));
        assertThat(details.os().version(), equalTo("12.2"));

        assertThat(details.device(), notNullValue());
        assertThat(details.device().name(), equalTo("iPad"));

        assertThat(details.deviceType(), equalTo("Tablet"));
    }

    public void testSpider() {
        Details details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (compatible; EasouSpider; +http://www.easou.com/search/spider.html)",
            true
        );

        assertThat(details.name(), equalTo("EasouSpider"));
        assertThat(details.version(), nullValue());

        assertThat(details.os(), nullValue());
        assertThat(details.osFull(), nullValue());

        assertThat(details.device(), notNullValue());
        assertThat(details.device().name(), equalTo("Spider"));

        assertThat(details.deviceType(), equalTo("Robot"));
    }

    public void testUnknownAgent() {
        Details details = parser.parseUserAgentInfo("Something I made up v42.0.1", true);

        assertThat(details.name(), nullValue());
        assertThat(details.version(), nullValue());
        assertThat(details.os(), nullValue());
        assertThat(details.osFull(), nullValue());
        assertThat(details.device(), nullValue());
        assertThat(details.deviceType(), equalTo("Other"));
    }

    public void testExtractDeviceTypeDisabled() {
        Details details = parser.parseUserAgentInfo(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            false
        );

        assertThat(details.name(), equalTo("Chrome"));
        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("Mac OS X"));
        assertThat(details.deviceType(), nullValue());
    }

    public void testCachedResultsAreReturned() {
        String ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36";
        Details first = parser.parseUserAgentInfo(ua, true);
        Details second = parser.parseUserAgentInfo(ua, true);
        assertSame(first, second);
    }

    public void testCacheIncludingDeviceTypes() {
        String agentString = "Mozilla/5.0 (Linux; U; Android 3.0; en-us; Xoom Build/HRI39) AppleWebKit/525.10+ (KHTML, like Gecko) "
            + "Version/3.0.5 Mobile Safari/523.12.2";
        Details first = parser.parseUserAgentInfo(agentString, false);
        assertNull(first.deviceType());
        Details second = parser.parseUserAgentInfo(agentString, true);
        assertThat(second.deviceType(), equalTo("Phone"));
        assertNotSame(first, second);
        Details third = parser.parseUserAgentInfo(agentString, false);
        assertSame(first, third);
    }

    public void testVersionToStringMajorOnly() {
        assertThat(UserAgentParserImpl.versionToString("10", null, null, null), equalTo("10"));
    }

    public void testVersionToStringMajorMinor() {
        assertThat(UserAgentParserImpl.versionToString("10", "9", null, null), equalTo("10.9"));
    }

    public void testVersionToStringMajorMinorPatch() {
        assertThat(UserAgentParserImpl.versionToString("10", "9", "2", null), equalTo("10.9.2"));
    }

    public void testVersionToStringFull() {
        assertThat(UserAgentParserImpl.versionToString("33", "0", "1750", "149"), equalTo("33.0.1750.149"));
    }

    public void testVersionToStringNullMajor() {
        assertThat(UserAgentParserImpl.versionToString(null, "1", "2", "3"), nullValue());
    }

    public void testVersionToStringEmptyMajor() {
        assertThat(UserAgentParserImpl.versionToString("", "1", "2", "3"), nullValue());
    }

    public void testVersionToStringSkipsPatchWhenMinorMissing() {
        assertThat(UserAgentParserImpl.versionToString("10", null, "2", null), equalTo("10"));
    }

    public void testVersionToStringSkipsBuildWhenPatchMissing() {
        assertThat(UserAgentParserImpl.versionToString("10", "9", null, "5"), equalTo("10.9"));
    }

    public void testInvalidRegexFileThrows() {
        byte[] invalidYaml = "not_a_valid_regex_file: true\n".getBytes(StandardCharsets.UTF_8);
        expectThrows(
            ElasticsearchParseException.class,
            () -> new UserAgentParserImpl("bad", new ByteArrayInputStream(invalidYaml), null, new UserAgentCache(10))
        );
    }

    public void testNullDeviceTypeRegexStream() {
        InputStream regexStream = UserAgentPlugin.class.getResourceAsStream("/regexes.yml");
        assertNotNull(regexStream);
        UserAgentParserImpl parserWithoutDeviceType = new UserAgentParserImpl("no-device", regexStream, null, new UserAgentCache(10));

        Details details = parserWithoutDeviceType.parseUserAgentInfo(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.149 Safari/537.36",
            true
        );

        assertThat(details.name(), equalTo("Chrome"));
        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("Mac OS X"));
        assertThat(details.deviceType(), nullValue());
    }

    public void testOsMatchedWithoutVersion() {
        Details details = parser.parseUserAgentInfo("Wget/1.21 (Ubuntu)", true);

        assertThat(details.name(), equalTo("Wget"));

        assertThat(details.os(), notNullValue());
        assertThat(details.os().name(), equalTo("Ubuntu"));
        assertThat(details.os().version(), nullValue());
        assertThat(details.osFull(), nullValue());
    }

    public void testSubpatternMatchWithReplacements() {
        var pattern = new UserAgentParserImpl.UserAgentSubpattern(
            java.util.regex.Pattern.compile("(MyBrowser)/(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)"),
            null,
            null,
            null,
            null,
            null
        );
        var result = pattern.match("MyBrowser/1.2.3.4");
        assertThat(result, notNullValue());
        assertThat(result.name(), equalTo("MyBrowser"));
        assertThat(result.version(), equalTo("1.2.3.4"));
    }

    public void testSubpatternMatchWithNameReplacement() {
        var pattern = new UserAgentParserImpl.UserAgentSubpattern(
            java.util.regex.Pattern.compile("(MyBrowser)/(\\d+)"),
            "ReplacedName",
            null,
            null,
            null,
            null
        );
        var result = pattern.match("MyBrowser/42");
        assertThat(result, notNullValue());
        assertThat(result.name(), equalTo("ReplacedName"));
        assertThat(result.version(), equalTo("42"));
    }

    public void testSubpatternMatchWithNameReplacementContainingGroupRef() {
        var pattern = new UserAgentParserImpl.UserAgentSubpattern(
            java.util.regex.Pattern.compile("(MyBrowser)/(\\d+)"),
            "$1 Mobile",
            null,
            null,
            null,
            null
        );
        var result = pattern.match("MyBrowser/42");
        assertThat(result, notNullValue());
        assertThat(result.name(), equalTo("MyBrowser Mobile"));
    }

    public void testSubpatternMatchWithVersionReplacements() {
        var pattern = new UserAgentParserImpl.UserAgentSubpattern(
            java.util.regex.Pattern.compile("(MyBrowser)"),
            null,
            "99",
            "88",
            "77",
            "66"
        );
        var result = pattern.match("MyBrowser");
        assertThat(result, notNullValue());
        assertThat(result.name(), equalTo("MyBrowser"));
        assertThat(result.version(), equalTo("99.88.77.66"));
    }

    public void testSubpatternNoMatch() {
        var pattern = new UserAgentParserImpl.UserAgentSubpattern(
            java.util.regex.Pattern.compile("(NoSuchBrowser)"),
            null,
            null,
            null,
            null,
            null
        );
        assertThat(pattern.match("Chrome/1.0"), nullValue());
    }

    public void testSubpatternCaseInsensitiveFlag() {
        var pattern = new UserAgentParserImpl.UserAgentSubpattern(
            java.util.regex.Pattern.compile("(mybrowser)", java.util.regex.Pattern.CASE_INSENSITIVE),
            null,
            null,
            null,
            null,
            null
        );
        var result = pattern.match("MyBrowser/1.0");
        assertThat(result, notNullValue());
        assertThat(result.name(), equalTo("MyBrowser"));
    }

    public void testGetName() {
        assertThat(parser.getName(), equalTo("_default"));
    }

    public void testPatternsLoadedFromRegexFile() {
        assertThat(parser.getUaPatterns().isEmpty(), equalTo(false));
        assertThat(parser.getOsPatterns().isEmpty(), equalTo(false));
        assertThat(parser.getDevicePatterns().isEmpty(), equalTo(false));
    }
}
