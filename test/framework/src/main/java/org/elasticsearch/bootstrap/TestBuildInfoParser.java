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

public class TestBuildInfoParser {

    private static final String PLUGIN_TEST_BUILD_INFO_RESOURCES = "META-INF/plugin-test-build-info.json";
    private static final String LIB_TEST_BUILD_INFO_RESOURCES = "META-INF/lib-test-build-info.json";
    private static final String SERVER_TEST_BUILD_INFO_RESOURCE = "META-INF/server-test-build-info.json";

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("test_build_info", Builder::new);
    private static final ObjectParser<Location, Void> LOCATION_PARSER = new ObjectParser<>("location", Location::new);
    static {
        LOCATION_PARSER.declareString(Location::representativeClass, new ParseField("representative_class"));
        LOCATION_PARSER.declareString(Location::module, new ParseField("module"));

        PARSER.declareString(Builder::component, new ParseField("component"));
        PARSER.declareObjectArray(Builder::locations, LOCATION_PARSER, new ParseField("locations"));
    }

    private static class Location {
        private String representativeClass;
        private String module;

        public void module(final String module) {
            this.module = module;
        }

        public void representativeClass(final String representativeClass) {
            this.representativeClass = representativeClass;
        }
    }

    private static final class Builder {
        private String component;
        private List<Location> locations;

        public void component(final String component) {
            this.component = component;
        }

        public void locations(final List<Location> locations) {
            this.locations = locations;
        }

        TestBuildInfo build() {
            return new TestBuildInfo(
                component,
                locations.stream().map(l -> new TestBuildInfoLocation(l.representativeClass, l.module)).toList()
            );
        }
    }

    static TestBuildInfo fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }

    public static List<TestBuildInfo> parseAllPluginTestBuildInfo() throws IOException {
        return parseAll(PLUGIN_TEST_BUILD_INFO_RESOURCES);
    }

    private static List<TestBuildInfo> parseAll(String resourceName) throws IOException {
        var xContent = XContentFactory.xContent(XContentType.JSON);
        List<TestBuildInfo> testBuildInfos = new ArrayList<>();
        var resources = TestBuildInfoParser.class.getClassLoader().getResources(resourceName);
        while (resources.hasMoreElements()) {
            try (
                var stream = getStream(resources.nextElement());
                var parser = xContent.createParser(XContentParserConfiguration.EMPTY, stream)
            ) {
                testBuildInfos.add(fromXContent(parser));
            }
        }
        return testBuildInfos;
    }

    public static TestBuildInfo parseServerTestBuildInfo() throws IOException {
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

    /**
     * Parse the server build-info and fold in the locations of every self-representing library on the classpath.
     * Foreign-library modules ({@code libs/zstd}, {@code libs/native}, {@code libs/simdvec}) emit a
     * {@code lib-test-build-info.json} so their annotation-processor generated {@code $Impl}/{@code $Provider}
     * classes — which no other build-info records — get a location; see {@code ForeignLibraryPlugin}. These
     * libraries are part of the server trust boundary and are entitled through server scopes keyed on their module
     * name, so their locations join the server build-info and are resolved with server scope.
     */
    public static TestBuildInfo parseServerAndLibTestBuildInfo() throws IOException {
        var serverTestBuildInfo = parseServerTestBuildInfo();
        if (serverTestBuildInfo == null) {
            return null;
        }
        var libsTestBuildInfo = parseAll(LIB_TEST_BUILD_INFO_RESOURCES);
        if (libsTestBuildInfo.isEmpty()) {
            return serverTestBuildInfo;
        }
        List<TestBuildInfoLocation> locations = new ArrayList<>(serverTestBuildInfo.locations());
        for (var libTestBuildInfo : libsTestBuildInfo) {
            locations.addAll(libTestBuildInfo.locations());
        }
        return new TestBuildInfo(serverTestBuildInfo.component(), locations);
    }

    @SuppressForbidden(reason = "URLs from class loader")
    private static InputStream getStream(URL resource) throws IOException {
        return resource.openStream();
    }
}
