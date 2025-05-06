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

    private static final String NAME_KEY = "name";
    private static final String LOCATIONS_KEY = "locations";

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

    // TODO: possibly move it to whoever is calling/using this
    // TODO: server test build info
    static List<TestBuildInfo> parseAllPluginTestBuildInfo() throws IOException {
        var xContent = XContentFactory.xContent(XContentType.JSON);
        List<TestBuildInfo> pluginsTestBuildInfos = new ArrayList<>();
        var resources = TestBuildInfoParser.class.getClassLoader().getResources("/META-INF/es-plugins/");
        URL resource;
        while ((resource = resources.nextElement()) != null) {
            try (var stream = getStream(resource); var parser = xContent.createParser(XContentParserConfiguration.EMPTY, stream)) {
                pluginsTestBuildInfos.add(fromXContent(parser));
            }
        }
        return pluginsTestBuildInfos;
    }

    @SuppressForbidden(reason = "URLs from class loader")
    private static InputStream getStream(URL resource) throws IOException {
        return resource.openStream();
    }
}
