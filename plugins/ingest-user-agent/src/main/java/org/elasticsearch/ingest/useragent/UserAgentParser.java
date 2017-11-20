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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
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
    private final List<UserAgentSubpattern> uaPatterns = new ArrayList<>();
    private final List<UserAgentSubpattern> osPatterns = new ArrayList<>();
    private final List<UserAgentSubpattern> devicePatterns = new ArrayList<>();
    private final String name;

    UserAgentParser(String name, InputStream regexStream, UserAgentCache cache) {
        this.name = name;
        this.cache = cache;

        try {
            init(regexStream);
        } catch (IOException e) {
            throw new ElasticsearchParseException("error parsing regular expression file", e);
        }
    }

    private void init(InputStream regexStream) throws IOException {
        // EMPTY is safe here because we don't use namedObject
        XContentParser yamlParser = XContentFactory.xContent(XContentType.YAML).createParser(NamedXContentRegistry.EMPTY, regexStream);

        XContentParser.Token token = yamlParser.nextToken();

        if (token == XContentParser.Token.START_OBJECT) {
            token = yamlParser.nextToken();

            for (; token != null; token = yamlParser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME && yamlParser.currentName().equals("user_agent_parsers")) {
                    List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);

                    for (Map<String, String> map : parserConfigurations) {
                        uaPatterns.add(new UserAgentSubpattern(compilePattern(map.get("regex"), map.get("regex_flag")),
                                map.get("family_replacement"), map.get("v1_replacement"), map.get("v2_replacement"),
                                map.get("v3_replacement"), map.get("v4_replacement")));
                    }
                }
                else if (token == XContentParser.Token.FIELD_NAME && yamlParser.currentName().equals("os_parsers")) {
                    List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);

                    for (Map<String, String> map : parserConfigurations) {
                        osPatterns.add(new UserAgentSubpattern(compilePattern(map.get("regex"), map.get("regex_flag")),
                                map.get("os_replacement"), map.get("os_v1_replacement"), map.get("os_v2_replacement"),
                                map.get("os_v3_replacement"), map.get("os_v4_replacement")));
                    }
                }
                else if (token == XContentParser.Token.FIELD_NAME && yamlParser.currentName().equals("device_parsers")) {
                    List<Map<String, String>> parserConfigurations = readParserConfigurations(yamlParser);

                    for (Map<String, String> map : parserConfigurations) {
                        devicePatterns.add(new UserAgentSubpattern(compilePattern(map.get("regex"), map.get("regex_flag")),
                                map.get("device_replacement"), null, null, null, null));
                    }
                }
            }
        }

        if (uaPatterns.isEmpty() && osPatterns.isEmpty() && devicePatterns.isEmpty()) {
            throw new ElasticsearchParseException("not a valid regular expression file");
        }
    }

    private Pattern compilePattern(String regex, String regex_flag) {
        // Only flag present in the current default regexes.yaml
        if (regex_flag != null && regex_flag.equals("i")) {
            return Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        } else {
            return Pattern.compile(regex);
        }
    }

    private List<Map<String, String>> readParserConfigurations(XContentParser yamlParser) throws IOException {
        List <Map<String, String>> patternList = new ArrayList<>();

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

    public Details parse(String agentString) {
        Details details = cache.get(name, agentString);;

        if (details == null) {
            VersionedName userAgent = findMatch(uaPatterns, agentString);
            VersionedName operatingSystem = findMatch(osPatterns, agentString);
            VersionedName device = findMatch(devicePatterns, agentString);

            details = new Details(userAgent, operatingSystem, device);

            cache.put(name, agentString, details);
        }

        return details;
    }

    private VersionedName findMatch(List<UserAgentSubpattern> possiblePatterns, String agentString) {
        VersionedName name;
        for (UserAgentSubpattern pattern : possiblePatterns) {
            name = pattern.match(agentString);

            if (name != null) {
                return name;
            }
        }

        return null;
    }

    static final class Details {
        public final VersionedName userAgent;
        public final VersionedName operatingSystem;
        public final VersionedName device;

        Details(VersionedName userAgent, VersionedName operatingSystem, VersionedName device) {
            this.userAgent = userAgent;
            this.operatingSystem = operatingSystem;
            this.device = device;
        }
    }

    static final class VersionedName {
        public final String name;
        public final String major;
        public final String minor;
        public final String patch;
        public final String build;

        VersionedName(String name, String major, String minor, String patch, String build) {
            this.name = name;
            this.major = major;
            this.minor = minor;
            this.patch = patch;
            this.build = build;
        }
    }

    /**
     * One of: user agent, operating system, device
     */
    static final class UserAgentSubpattern {
        private final Pattern pattern;
        private final String nameReplacement, v1Replacement, v2Replacement, v3Replacement, v4Replacement;

        UserAgentSubpattern(Pattern pattern, String nameReplacement,
                String v1Replacement, String v2Replacement, String v3Replacement, String v4Replacement) {
          this.pattern = pattern;
          this.nameReplacement = nameReplacement;
          this.v1Replacement = v1Replacement;
          this.v2Replacement = v2Replacement;
          this.v3Replacement = v3Replacement;
          this.v4Replacement = v4Replacement;
        }

        public VersionedName match(String agentString) {
          String name = null, major = null, minor = null, patch = null, build = null;
          Matcher matcher = pattern.matcher(agentString);

          if (!matcher.find()) {
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
