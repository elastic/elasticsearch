/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.useragent.UserAgentParser.VersionedName;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.ingest.useragent.UserAgentParser.readParserConfigurations;

public class DeviceTypeParser {

    private static final String OS_PARSERS = "os_parsers";
    private static final String BROWSER_PARSER = "browser_parsers";
    private static final String DEVICE_PARSER = "device_parsers";
    private static final String AGENT_STRING_PARSER = "agent_string_parsers";
    private static final String robot = "Robot", tablet = "Tablet", desktop = "Desktop", phone = "Phone";

    private final List<String> patternListKeys = List.of(OS_PARSERS, BROWSER_PARSER, DEVICE_PARSER, AGENT_STRING_PARSER);

    private final HashMap<String, ArrayList<DeviceTypeSubPattern>> deviceTypePatterns = new HashMap<>();

    public void init(InputStream regexStream) throws IOException {
        XContentParser yamlParser = XContentFactory.xContent(XContentType.YAML)
            .createParser(XContentParserConfiguration.EMPTY, regexStream);

        XContentParser.Token token = yamlParser.nextToken();

        if (token == XContentParser.Token.START_OBJECT) {
            token = yamlParser.nextToken();

            for (; token != null; token = yamlParser.nextToken()) {
                String currentName = yamlParser.currentName();
                if (token == XContentParser.Token.FIELD_NAME && patternListKeys.contains(currentName)) {
                    List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);
                    ArrayList<DeviceTypeSubPattern> subPatterns = new ArrayList<>();
                    for (Map<String, String> map : parserConfigurations) {
                        subPatterns.add(new DeviceTypeSubPattern(Pattern.compile((map.get("regex"))), map.get("replacement")));
                    }
                    deviceTypePatterns.put(currentName, subPatterns);
                }
            }
        }

        if (patternListKeys.size() != deviceTypePatterns.size()) {
            throw new ElasticsearchParseException("not a valid regular expression file");
        }
    }

    public String findDeviceType(String agentString, VersionedName userAgent, VersionedName os, VersionedName device) {
        if (deviceTypePatterns.isEmpty()) {
            return null;
        }
        if (agentString != null) {
            String deviceType = findMatch(deviceTypePatterns.get(AGENT_STRING_PARSER), agentString);
            if (deviceType != null) {
                return deviceType;
            }
        }
        return findDeviceType(userAgent, os, device);
    }

    public String findDeviceType(VersionedName userAgent, VersionedName os, VersionedName device) {

        if (deviceTypePatterns.isEmpty()) {
            return null;
        }

        ArrayList<String> extractedDeviceTypes = new ArrayList<>();

        for (String patternKey : patternListKeys) {
            String deviceType = null;
            switch (patternKey) {
                case OS_PARSERS:
                    if (os != null && os.name() != null) {
                        deviceType = findMatch(deviceTypePatterns.get(patternKey), os.name());
                    }
                    break;
                case BROWSER_PARSER:
                    if (userAgent != null && userAgent.name() != null) {
                        deviceType = findMatch(deviceTypePatterns.get(patternKey), userAgent.name());
                    }
                    break;
                case DEVICE_PARSER:
                    if (device != null && device.name() != null) {
                        deviceType = findMatch(deviceTypePatterns.get(patternKey), device.name());
                    }
                    break;
                default:
                    break;
            }

            if (deviceType != null) {
                extractedDeviceTypes.add(deviceType);
            }
        }

        if (extractedDeviceTypes.contains(robot)) {
            return robot;
        }
        if (extractedDeviceTypes.contains(tablet)) {
            return tablet;
        }
        if (extractedDeviceTypes.contains(phone)) {
            return phone;
        }
        if (extractedDeviceTypes.contains(desktop)) {
            return desktop;
        }

        return "Other";
    }

    private String findMatch(List<DeviceTypeSubPattern> possiblePatterns, String matchString) {
        String name;
        for (DeviceTypeSubPattern pattern : possiblePatterns) {
            name = pattern.match(matchString);
            if (name != null) {
                return name;
            }
        }
        return null;
    }

    static final class DeviceTypeSubPattern {
        private final Pattern pattern;
        private final String nameReplacement;

        DeviceTypeSubPattern(Pattern pattern, String nameReplacement) {
            this.pattern = pattern;
            this.nameReplacement = nameReplacement;
        }

        public String match(String matchString) {
            String name = null;

            Matcher matcher = pattern.matcher(matchString);

            if (matcher.find() == false) {
                return null;
            }

            int groupCount = matcher.groupCount();

            if (nameReplacement != null) {
                if (nameReplacement.contains("$1") && groupCount >= 1 && matcher.group(1) != null) {
                    name = nameReplacement.replaceFirst("\\$1", Matcher.quoteReplacement(matcher.group(1)));
                } else {
                    name = nameReplacement;
                }
            }

            return name;
        }
    }

}
