/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.exception.ElasticsearchParseException;
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

final class UserAgentParser {

    private final UserAgentCache cache;
    private final DeviceTypeParser deviceTypeParser = new DeviceTypeParser();
    private final List<UserAgentSubpattern> uaPatterns = new ArrayList<>();
    private final List<UserAgentSubpattern> osPatterns = new ArrayList<>();
    private final List<UserAgentSubpattern> devicePatterns = new ArrayList<>();
    private final String name;

    UserAgentParser(String name, InputStream regexStream, InputStream deviceTypeRegexStream, UserAgentCache cache) {
        this.name = name;
        this.cache = cache;

        try {
            init(regexStream);
            if (deviceTypeRegexStream != null) {
                deviceTypeParser.init(deviceTypeRegexStream);
            }
        } catch (IOException e) {
            throw new ElasticsearchParseException("error parsing regular expression file", e);
        }
    }

    private void init(InputStream regexStream) throws IOException {
        // EMPTY is safe here because we don't use namedObject
        try (
            XContentParser yamlParser = XContentFactory.xContent(XContentType.YAML)
                .createParser(XContentParserConfiguration.EMPTY, regexStream)
        ) {

            XContentParser.Token token = yamlParser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                token = yamlParser.nextToken();

                for (; token != null; token = yamlParser.nextToken()) {
                    if (token == XContentParser.Token.FIELD_NAME && yamlParser.currentName().equals("user_agent_parsers")) {
                        List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);

                        for (Map<String, String> map : parserConfigurations) {
                            uaPatterns.add(
                                new UserAgentSubpattern(
                                    compilePattern(map.get("regex"), map.get("regex_flag")),
                                    map.get("family_replacement"),
                                    map.get("v1_replacement"),
                                    map.get("v2_replacement"),
                                    map.get("v3_replacement"),
                                    map.get("v4_replacement")
                                )
                            );
                        }
                    } else if (token == XContentParser.Token.FIELD_NAME && yamlParser.currentName().equals("os_parsers")) {
                        List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);

                        for (Map<String, String> map : parserConfigurations) {
                            osPatterns.add(
                                new UserAgentSubpattern(
                                    compilePattern(map.get("regex"), map.get("regex_flag")),
                                    map.get("os_replacement"),
                                    map.get("os_v1_replacement"),
                                    map.get("os_v2_replacement"),
                                    map.get("os_v3_replacement"),
                                    map.get("os_v4_replacement")
                                )
                            );
                        }
                    } else if (token == XContentParser.Token.FIELD_NAME && yamlParser.currentName().equals("device_parsers")) {
                        List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);

                        for (Map<String, String> map : parserConfigurations) {
                            devicePatterns.add(
                                new UserAgentSubpattern(
                                    compilePattern(map.get("regex"), map.get("regex_flag")),
                                    map.get("device_replacement"),
                                    null,
                                    null,
                                    null,
                                    null
                                )
                            );
                        }
                    }
                }
            }
        }

        if (uaPatterns.isEmpty() && osPatterns.isEmpty() && devicePatterns.isEmpty()) {
            throw new ElasticsearchParseException("not a valid regular expression file");
        }
    }

    private static Pattern compilePattern(String regex, String regex_flag) {
        // Only flag present in the current default regexes.yaml
        if (regex_flag != null && regex_flag.equals("i")) {
            return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        } else {
            return Pattern.compile(regex);
        }
    }

    static List<Map<String, String>> readParserConfigurations(XContentParser yamlParser) throws IOException {
        List<Map<String, String>> patternList = new ArrayList<>();

        XContentParser.Token token = yamlParser.nextToken();
        if (token != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("malformed regular expression file, should continue with 'array' after 'object'");
        }

        token = yamlParser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("malformed regular expression file, expecting 'object'");
        }

        while (token == XContentParser.Token.START_OBJECT) {
            token = yamlParser.nextToken();

            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ElasticsearchParseException("malformed regular expression file, should continue with 'field_name' after 'array'");
            }

            Map<String, String> regexMap = new HashMap<>();
            for (; token == XContentParser.Token.FIELD_NAME; token = yamlParser.nextToken()) {
                String fieldName = yamlParser.currentName();

                token = yamlParser.nextToken();
                String fieldValue = yamlParser.text();
                regexMap.put(fieldName, fieldValue);
            }

            patternList.add(regexMap);

            token = yamlParser.nextToken();
        }

        return patternList;
    }

    List<UserAgentSubpattern> getUaPatterns() {
        return uaPatterns;
    }

    List<UserAgentSubpattern> getOsPatterns() {
        return osPatterns;
    }

    List<UserAgentSubpattern> getDevicePatterns() {
        return devicePatterns;
    }

    String getName() {
        return name;
    }

    public Details parse(String agentString, boolean extractDeviceType) {
        Details details = cache.get(name, agentString);

        if (details == null) {
            VersionedName userAgent = findMatch(uaPatterns, agentString);
            VersionedName operatingSystem = findMatch(osPatterns, agentString);
            VersionedName device = findMatch(devicePatterns, agentString);
            String deviceType = extractDeviceType ? deviceTypeParser.findDeviceType(agentString, userAgent, operatingSystem, device) : null;
            details = new Details(userAgent, operatingSystem, device, deviceType);
            cache.put(name, agentString, details);
        }

        return details;
    }

    private static VersionedName findMatch(List<UserAgentSubpattern> possiblePatterns, String agentString) {
        VersionedName versionedName;
        for (UserAgentSubpattern pattern : possiblePatterns) {
            versionedName = pattern.match(agentString);

            if (versionedName != null) {
                return versionedName;
            }
        }

        return null;
    }

    record Details(VersionedName userAgent, VersionedName operatingSystem, VersionedName device, String deviceType) {}

    record VersionedName(String name, String major, String minor, String patch, String build) {}

    /**
     * One of: user agent, operating system, device
     */
    record UserAgentSubpattern(
        Pattern pattern,
        String nameReplacement,
        String v1Replacement,
        String v2Replacement,
        String v3Replacement,
        String v4Replacement
    ) {

        public VersionedName match(String agentString) {
            String name = null, major = null, minor = null, patch = null, build = null;
            Matcher matcher = pattern.matcher(agentString);

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
            } else if (groupCount >= 1) {
                name = matcher.group(1);
            }

            if (v1Replacement != null) {
                major = v1Replacement;
            } else if (groupCount >= 2) {
                major = matcher.group(2);
            }

            if (v2Replacement != null) {
                minor = v2Replacement;
            } else if (groupCount >= 3) {
                minor = matcher.group(3);
            }

            if (v3Replacement != null) {
                patch = v3Replacement;
            } else if (groupCount >= 4) {
                patch = matcher.group(4);
            }

            if (v4Replacement != null) {
                build = v4Replacement;
            } else if (groupCount >= 5) {
                build = matcher.group(5);
            }

            return name == null ? null : new VersionedName(name, major, minor, patch, build);
        }
    }
}
