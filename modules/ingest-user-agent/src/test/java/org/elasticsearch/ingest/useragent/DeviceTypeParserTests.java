/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.elasticsearch.ingest.useragent.UserAgentParser.VersionedName;


import static org.elasticsearch.ingest.useragent.UserAgentParser.readParserConfigurations;
import static org.hamcrest.Matchers.*;

public class DeviceTypeParserTests extends ESTestCase {

    private static DeviceTypeParser deviceTypeParser;


    private ArrayList<HashMap<String, String>> readTestDevices(InputStream regexStream, String keyName) throws IOException {
        XContentParser yamlParser = XContentFactory.xContent(XContentType.YAML).createParser(NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE, regexStream);

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
                        testDevices.add(testDevice);

                    }
                }
            }
        }

        return testDevices;
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
        VersionedName os = new VersionedName("Mac OS X");

        VersionedName userAgent = new VersionedName("Chrome");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

        assertThat(deviceType, is("Desktop"));
    }

    @SuppressWarnings("unchecked")
    public void testAndroidMobile() throws Exception {

        VersionedName os = new VersionedName("iOS");

        VersionedName userAgent = new VersionedName("Safari");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

        assertThat(deviceType, is("Mobile"));
    }

    @SuppressWarnings("unchecked")
    public void testIPadTablet() throws Exception {

        VersionedName os = new VersionedName("iOS");

        VersionedName userAgent = new VersionedName("Safari");

        VersionedName device = new VersionedName("iPad");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, device);

        assertThat(deviceType, is("Tablet"));
    }

    @SuppressWarnings("unchecked")
    public void testWindowDesktop() throws Exception {

        VersionedName os = new VersionedName("Mac OS X");

        VersionedName userAgent = new VersionedName("Chrome");

        String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

        assertThat(deviceType, is("Desktop"));
    }

    @SuppressWarnings("unchecked")
    public void testRobotDevices() throws Exception {

        InputStream deviceTypeRegexStream = IngestUserAgentPlugin.class.getResourceAsStream("/robot-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(deviceTypeRegexStream, "robot_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = new VersionedName(testDevice.get("os"));

            VersionedName userAgent = new VersionedName(testDevice.get("browser"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

            assertThat(deviceType, is("Robot"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testDesktopDevices() throws Exception {

        InputStream deviceTypeRegexStream = IngestUserAgentPlugin.class.getResourceAsStream("/desktop-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(deviceTypeRegexStream, "desktop_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = new VersionedName(testDevice.get("os"));

            VersionedName userAgent = new VersionedName(testDevice.get("browser"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);
            if (deviceType.equals("Mobile")) {
                String name = "wow";
            }
            assertThat(deviceType, is("Desktop"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testMobileDevices() throws Exception {

        InputStream deviceTypeRegexStream = IngestUserAgentPlugin.class.getResourceAsStream("/mobile-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(deviceTypeRegexStream, "mobile_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = new VersionedName(testDevice.get("os"));

            VersionedName userAgent = new VersionedName(testDevice.get("browser"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

            assertThat(deviceType, is("Mobile"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testTabletDevices() throws Exception {

        InputStream deviceTypeRegexStream = IngestUserAgentPlugin.class.getResourceAsStream("/tablet-devices.yml");

        ArrayList<HashMap<String, String>> testDevices = readTestDevices(deviceTypeRegexStream, "tablet_devices");

        for (HashMap<String, String> testDevice : testDevices) {
            VersionedName os = new VersionedName(testDevice.get("os"));

            VersionedName userAgent = new VersionedName(testDevice.get("browser"));

            String deviceType = deviceTypeParser.findDeviceType(userAgent, os, null);

            assertThat(deviceType, is("Tablet"));
        }
    }

}
