/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class TestBuildInfoParser {

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
            return new TestBuildInfo(name);
        }
    }


    public static TestBuildInfo fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null).build();
    }
}
