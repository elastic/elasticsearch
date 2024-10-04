/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.policy;

import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;

public class PolicyBuilder {

    public static ParseField POLICY_PARSEFIELD = new ParseField("policy");
    public static ParseField MODULE_PARSEFIELD = new ParseField("module");
    public static ParseField NAME_PARSEFIELD = new ParseField("name");
    public static ParseField ENTITLEMENTS_PARSEFIELD = new ParseField("entitlements");

    /*
    START_OBJECT
    FIELD_NAME: policy
    START_OBJECT
    FIELD_NAME: module
    START_OBJECT
    FIELD_NAME: name
    VALUE_STRING: entitlement-test
    FIELD_NAME: entitlements
    START_ARRAY
    START_OBJECT
    FIELD_NAME: file
    START_OBJECT
    FIELD_NAME: path
    VALUE_STRING: test/path/to/file
    FIELD_NAME: actions
    START_ARRAY
    VALUE_STRING: read
    VALUE_STRING: write
    END_ARRAY
    END_OBJECT
    END_OBJECT
    END_ARRAY
    END_OBJECT
    END_OBJECT
    END_OBJECT
    */

    public static void parsePolicy(String name, InputStream is) {
        try {
            XContentParser parser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, is);
            parser.nextToken();
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected policy [" + name + "] to specify 'policy: ...'");
            }
            parser.nextToken();
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME || parser.currentName().equals(POLICY_PARSEFIELD.getPreferredName()) == false) {
                throw new IllegalArgumentException("expected policy [" + name + "] to specify 'policy: ...'");
            }
            parser.nextToken();
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new IllegalArgumentException("expected policy [" + name + "] to specify 'module: ...'");
            }
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException("expected policy [" + name + "] to specify 'module: ...'");
                }
                parser.nextToken();
                if (parser.currentToken() != XContentParser.Token.FIELD_NAME || parser.currentName().equals(MODULE_PARSEFIELD.getPreferredName()) == false) {
                    throw new IllegalArgumentException("expected policy [" + name + "] to specify 'module: ...'");
                }
                parser.nextToken();
                if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException("expected policy [" + name + "] to specify 'module: ...'");
                }
                parseModulePolicy(name, parser);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static void parseModulePolicy(String name, XContentParser parser) throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("expected policy [" + name + "] to specify 'module: ...'");
            }
            String currentFieldName = parser.currentName();
            if (currentFieldName.equals(NAME_PARSEFIELD.getPreferredName())) {
                parser.nextToken();
                if (parser.currentToken() != XContentParser.Token.VALUE_STRING) {
                    throw new IllegalArgumentException("expected policy [" + name + "] to specify value for 'name: <name>'");
                }
                // TODO: store module name
            } else if (currentFieldName.equals(ENTITLEMENTS_PARSEFIELD.getPreferredName())) {
                parser.nextToken();
                if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException("expected policy [" + name + "] to specify 'entitlements: [...]'");
                }
                parseEntitlements(name, parser);
            } else {
                throw new IllegalArgumentException("unexpected field name [" + currentFieldName + "] for policy [" + name + "]");
            }
        }
    }

    private static void parseEntitlements(String name, XContentParser parser) throws IOException {
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected policy [" + name + "] to specify '- <entitlement name>: ...'");
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("expected policy [" + name + "] to specify '- <entitlement name>: ...'");
            }
            String currentFieldName = parser.currentName();
            if (currentFieldName.equals("file")) {
                parseFileEntitlement(name, parser);
            }
        }
    }

    private static void parseFileEntitlement(String name, XContentParser parser) {

    }

    private PolicyBuilder() {
        // do nothing
    }
}
