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

package org.elasticsearch.client.migration;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class DeprecationInfoResponse {

    private static final ParseField CLUSTER_SETTINGS = new ParseField("cluster_settings");
    private static final ParseField NODE_SETTINGS = new ParseField("node_settings");
    private static final ParseField INDEX_SETTINGS = new ParseField("index_settings");
    private static final ParseField ML_SETTINGS = new ParseField("ml_settings");

    private final List<DeprecationIssue> clusterSettingsIssues;
    private final List<DeprecationIssue> nodeSettingsIssues;
    private final Map<String, List<DeprecationIssue>> indexSettingsIssues;
    private final List<DeprecationIssue> mlSettingsIssues;

    public DeprecationInfoResponse(List<DeprecationIssue> clusterSettingsIssues, List<DeprecationIssue> nodeSettingsIssues,
                                   Map<String, List<DeprecationIssue>> indexSettingsIssues, List<DeprecationIssue> mlSettingsIssues) {
        this.clusterSettingsIssues = Objects.requireNonNull(clusterSettingsIssues, "cluster settings issues cannot be null");
        this.nodeSettingsIssues = Objects.requireNonNull(nodeSettingsIssues, "node settings issues cannot be null");
        this.indexSettingsIssues = Objects.requireNonNull(indexSettingsIssues, "index settings issues cannot be null");
        this.mlSettingsIssues = Objects.requireNonNull(mlSettingsIssues, "ml settings issues cannot be null");
    }

    public List<DeprecationIssue> getClusterSettingsIssues() {
        return clusterSettingsIssues;
    }

    public List<DeprecationIssue> getNodeSettingsIssues() {
        return nodeSettingsIssues;
    }

    public Map<String, List<DeprecationIssue>> getIndexSettingsIssues() {
        return indexSettingsIssues;
    }

    public List<DeprecationIssue> getMlSettingsIssues() {
        return mlSettingsIssues;
    }

    private static List<DeprecationIssue> parseDeprecationIssues(XContentParser parser) throws IOException {
        List<DeprecationIssue> issues = new ArrayList<>();
        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.START_OBJECT) {
                issues.add(DeprecationIssue.PARSER.parse(parser, null));
            }
        }
        return issues;
    }

    public static DeprecationInfoResponse fromXContent(XContentParser parser) throws IOException {
        Map<String, List<DeprecationIssue>> indexSettings = new HashMap<>();
        List<DeprecationIssue> clusterSettings = new ArrayList<>();
        List<DeprecationIssue> nodeSettings = new ArrayList<>();
        List<DeprecationIssue> mlSettings = new ArrayList<>();
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (CLUSTER_SETTINGS.getPreferredName().equals(fieldName)) {
                clusterSettings.addAll(parseDeprecationIssues(parser));
            } else if (NODE_SETTINGS.getPreferredName().equals(fieldName)) {
                nodeSettings.addAll(parseDeprecationIssues(parser));
            } else if (ML_SETTINGS.getPreferredName().equals(fieldName)) {
                mlSettings.addAll(parseDeprecationIssues(parser));
            } else if (INDEX_SETTINGS.getPreferredName().equals(fieldName)) {
                // parse out the key/value pairs
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    String key = parser.currentName();
                    List<DeprecationIssue> value = parseDeprecationIssues(parser);
                    if (value.size() > 0) { // only add indices that contain deprecation issues
                        indexSettings.put(key, value);
                    }
                }
            }
        }
        return new DeprecationInfoResponse(clusterSettings, nodeSettings, indexSettings, mlSettings);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeprecationInfoResponse that = (DeprecationInfoResponse) o;
        return Objects.equals(clusterSettingsIssues, that.clusterSettingsIssues) &&
            Objects.equals(nodeSettingsIssues, that.nodeSettingsIssues) &&
            Objects.equals(mlSettingsIssues, that.mlSettingsIssues) &&
            Objects.equals(indexSettingsIssues, that.indexSettingsIssues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterSettingsIssues, nodeSettingsIssues, indexSettingsIssues, mlSettingsIssues);
    }

    @Override
    public String toString() {
        return clusterSettingsIssues.toString() + ":" + nodeSettingsIssues.toString() + ":" + indexSettingsIssues.toString() +
            ":" + mlSettingsIssues.toString();
    }

    /**
     * Information about deprecated items
     */
    public static class DeprecationIssue {

        private static final ParseField LEVEL = new ParseField("level");
        private static final ParseField MESSAGE = new ParseField("message");
        private static final ParseField URL = new ParseField("url");
        private static final ParseField DETAILS = new ParseField("details");

        static final ConstructingObjectParser<DeprecationIssue, Void> PARSER =
            new ConstructingObjectParser<>("deprecation_issue", true,
                a -> new DeprecationIssue(Level.fromString((String) a[0]), (String) a[1], (String) a[2], (String) a[3]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), LEVEL);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), MESSAGE);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), URL);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DETAILS);
        }

        public enum Level {
            WARNING,
            CRITICAL
            ;

            public static Level fromString(String value) {
                return Level.valueOf(value.toUpperCase(Locale.ROOT));
            }

            @Override
            public String toString() {
                return name().toLowerCase(Locale.ROOT);
            }
        }

        private Level level;
        private String message;
        private String url;
        private String details;

        public DeprecationIssue(Level level, String message, String url, @Nullable String details) {
            this.level = level;
            this.message = message;
            this.url = url;
            this.details = details;
        }

        public Level getLevel() {
            return level;
        }

        public String getMessage() {
            return message;
        }

        public String getUrl() {
            return url;
        }

        public String getDetails() {
            return details;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeprecationIssue that = (DeprecationIssue) o;
            return Objects.equals(level, that.level) &&
                Objects.equals(message, that.message) &&
                Objects.equals(url, that.url) &&
                Objects.equals(details, that.details);
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, message, url, details);
        }
    }
}
