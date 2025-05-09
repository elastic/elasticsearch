/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

class TestBuildInfoParser {

    private static final String PLUGIN_TEST_BUILD_INFO_RESOURCES = "META-INF/plugin-test-build-info.json";
    private static final String SERVER_TEST_BUILD_INFO_RESOURCE = "META-INF/server-test-build-info.json";

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("test_build_info", Builder::new);
    private static final ObjectParser<Location, Void> LOCATION_PARSER = new ObjectParser<>("location", Location::new);
    static {
        LOCATION_PARSER.declareString(Location::className, new ParseField("class"));
        LOCATION_PARSER.declareString(Location::moduleName, new ParseField("module"));

        PARSER.declareString(Builder::name, new ParseField("name"));
        PARSER.declareObjectArray(Builder::locations, LOCATION_PARSER, new ParseField("locations"));
    }

    private static class Location {
        private String className;
        private String moduleName;

        public void moduleName(final String moduleName) {
            this.moduleName = moduleName;
        }

        public void className(final String className) {
            this.className = className;
        }
    }

    private static final class Builder {
        private String name;
        private List<Location> locations;

        public void name(final String name) {
            this.name = name;
        }

        public void locations(final List<Location> locations) {
            this.locations = locations;
        }

        TestBuildInfo build() {
            return new TestBuildInfo(name, locations.stream().map(l -> new TestBuildInfoLocation(l.className, l.moduleName)).toList());
        }
    }

    static TestBuildInfo fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    static List<TestBuildInfo> parseAllPluginTestBuildInfo() throws IOException {
        var xContent = XContentFactory.xContent(XContentType.JSON);
        List<TestBuildInfo> pluginsTestBuildInfos = new ArrayList<>();
        var resources = TestBuildInfoParser.class.getClassLoader().getResources(PLUGIN_TEST_BUILD_INFO_RESOURCES);
        URL resource;
        while ((resource = resources.nextElement()) != null) {
            try (var stream = getStream(resource); var parser = xContent.createParser(XContentParserConfiguration.EMPTY, stream)) {
                pluginsTestBuildInfos.add(fromXContent(parser));
            }
        }
        return pluginsTestBuildInfos;
    }

    static TestBuildInfo parseServerTestBuildInfo() throws IOException {
        var xContent = XContentFactory.xContent(XContentType.JSON);
        var resource = TestBuildInfoParser.class.getClassLoader().getResource(SERVER_TEST_BUILD_INFO_RESOURCE);
        // No test-build-info for server: this might be a non-gradle build. Proceed without TestBuildInfo
        if (resource == null) {
            return null;
        }
        try (var stream = getStream(resource); var parser = xContent.createParser(XContentParserConfiguration.EMPTY, stream)) {
            return fromXContent(parser);
        }
    }

    @SuppressForbidden(reason = "URLs from class loader")
    private static InputStream getStream(URL resource) throws IOException {
        return resource.openStream();
    }
}
