/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.useragent.UserAgentParser.VersionedName;
import static org.elasticsearch.ingest.useragent.UserAgentParser.readParserConfigurations;
import static org.hamcrest.Matchers.is;

public class DeviceTypeParserTests extends ESTestCase {

    private static DeviceTypeParser deviceTypeParser;

    private ArrayList<HashMap<String, String>> readTestDevices(InputStream regexStream, String keyName) throws IOException {
        XContentParser yamlParser = XContentFactory.xContent(XContentType.YAML)
            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, regexStream);

        XContentParser.Token token = yamlParser.nextToken();

        ArrayList<HashMap<String, String>> testDevices = new ArrayList<>();

        if (token == XContentParser.Token.START_OBJECT) {
            token = yamlParser.nextToken();

            for (; token != null; token = yamlParser.nextToken()) {
                String currentName = yamlParser.currentName();
                if (token == XContentParser.Token.FIELD_NAME && currentName.equals(keyName)) {
                    List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);

                    for (Map<String, String> map : parserConfigurations) {
                        HashMap<String, String> testDevice = new HashMap<>();

                        testDevice.put("type", map.get("type"));
                        testDevice.put("os", map.get("os"));
                        testDevice.put("browser", map.get("browser"));
                        testDevice.put("device", map.get("device"));
                        testDevices.add(testDevice);

                    }
                }
            }
        }

        return testDevices;
    }

    private static VersionedName getVersionName(String name) {
        return new VersionedName(name, null, null, null, null);
    }

    @BeforeClass
    public static void setupDeviceParser() throws IOException {
        InputStream deviceTypeRegexStream = UserAgentProcessor.class.getResourceAsStream("/device_type_regexes.yml");

        assertNotNull(deviceTypeRegexStream);
        assertNotNull(deviceTypeRegexStream);

        deviceTypeParser = new DeviceTypeParser();
        deviceTypeParser.init(deviceTypeRegexStream);
    }

    @SuppressWarnings("unchecked")
    public void testMacDesktop() throws Exception {
        VersionedName os = getVersionName("Mac OS X");

        VersionedName userAgent = getVersionName("Chrome");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

        assertThat(deviceType, is("Desktop"));
    }

    @SuppressWarnings("unchecked")
    public void testAndroidMobile() throws Exception {

        VersionedName os = getVersionName("iOS");

        VersionedName userAgent = getVersionName("Safari");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

        assertThat(deviceType, is("Phone"));
    }

    @SuppressWarnings("unchecked")
    public void testIPadTablet() throws Exception {

        VersionedName os = getVersionName("iOS");

        VersionedName userAgent = getVersionName("Safari");

        VersionedName device = getVersionName("iPad");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, device);

        assertThat(deviceType, is("Tablet"));
    }

    @SuppressWarnings("unchecked")
    public void testWindowDesktop() throws Exception {

        VersionedName os = getVersionName("Mac OS X");

        VersionedName userAgent = getVersionName("Chrome");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

        assertThat(deviceType, is("Desktop"));
    }

    @SuppressWarnings("unchecked")
    public void testRobotAgentString() throws Exception {

        String deviceType = deviceTypeParser.findDeviceType(
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:63.0.247) Gecko/20100101 Firefox/63.0.247 Site24x7",
            null,
            null,
            null
        );

        assertThat(deviceType, is("Robot"));
    }

    @SuppressWarnings("unchecked")
    public void testRobotDevices() throws Exception {

        InputStream testRobotDevices = IngestUserAgentPlugin.class.getResourceAsStream("/test-robot-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(testRobotDevices, "robot_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = getVersionName(testDevice.get("os"));

            VersionedName userAgent = getVersionName(testDevice.get("browser"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

            assertThat(deviceType, is("Robot"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testDesktopDevices() throws Exception {

        InputStream testDesktopDevices = IngestUserAgentPlugin.class.getResourceAsStream("/test-desktop-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(testDesktopDevices, "desktop_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = getVersionName(testDevice.get("os"));

            VersionedName userAgent = getVersionName(testDevice.get("browser"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

            assertThat(deviceType, is("Desktop"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testMobileDevices() throws Exception {

        InputStream testMobileDevices = IngestUserAgentPlugin.class.getResourceAsStream("/test-mobile-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(testMobileDevices, "mobile_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = getVersionName(testDevice.get("os"));

            VersionedName userAgent = getVersionName(testDevice.get("browser"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

            assertThat(deviceType, is("Phone"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testTabletDevices() throws Exception {

        InputStream testTabletDevices = IngestUserAgentPlugin.class.getResourceAsStream("/test-tablet-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(testTabletDevices, "tablet_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = getVersionName(testDevice.get("os"));

            VersionedName userAgent = getVersionName(testDevice.get("browser"));

            VersionedName device = getVersionName(testDevice.get("device"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, device);

            assertThat(deviceType, is("Tablet"));
        }
    }

}
