/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.parser;

import org.elasticsearch.entitlement.runtime.policy.impl.FileEntitlement;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.entitlement.runtime.policy.parser.PolicyParserException.newPolicyParserException;

public class FileEntitlementParser extends EntitlementParser {

    public static ParseField FILE_PARSEFIELD = new ParseField("file");
    public static ParseField PATH_PARSEFIELD = new ParseField("path");
    public static ParseField ACTIONS_PARSEFIELD = new ParseField("actions");

    public static String READ_ACTION = "read";
    public static String WRITE_ACTION = "write";

    protected FileEntitlementParser(String policyName, String scopeName, XContentParser policyParser) {
        super(policyName, scopeName, policyParser);
    }

    protected FileEntitlement parseEntitlement() throws IOException {
        if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw newPolicyParserException(
                policyParser.getTokenLocation(),
                policyName,
                scopeName,
                "expected object [" + FILE_PARSEFIELD.getPreferredName() + "]"
            );
        }
        String path = null;
        int actions = 0;
        while (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (policyParser.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    scopeName,
                    "expected object [" + FILE_PARSEFIELD.getPreferredName() + "]"
                );
            }
            String currentFieldName = policyParser.currentName();
            if (currentFieldName.equals(PATH_PARSEFIELD.getPreferredName())) {
                if (policyParser.nextToken() != XContentParser.Token.VALUE_STRING) {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        scopeName,
                        FILE_PARSEFIELD.getPreferredName(),
                        "expected value for field [" + PATH_PARSEFIELD.getPreferredName() + "]"
                    );
                }
                if (path == null) {
                    path = policyParser.text();
                } else {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        "field ["
                            + PATH_PARSEFIELD.getPreferredName()
                            + "] has already been set; found multiple values"
                            + "['"
                            + path
                            + "', '"
                            + policyParser.text()
                            + "']"
                    );
                }
            } else if (currentFieldName.equals(ACTIONS_PARSEFIELD.getPreferredName())) {
                if (policyParser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        scopeName,
                        FILE_PARSEFIELD.getPreferredName(),
                        "expected array for field [" + ACTIONS_PARSEFIELD.getPreferredName() + "]"
                    );
                }
                while (policyParser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (policyParser.currentToken() != XContentParser.Token.VALUE_STRING) {
                        throw newPolicyParserException(
                            policyParser.getTokenLocation(),
                            policyName,
                            scopeName,
                            FILE_PARSEFIELD.getPreferredName(),
                            "expected value(s) for array [" + ACTIONS_PARSEFIELD.getPreferredName() + "]"
                        );
                    }
                    String action = policyParser.text();
                    if (READ_ACTION.equals(action)) {
                        if ((actions & FileEntitlement.READ_ACTION) == FileEntitlement.READ_ACTION) {
                            throw newPolicyParserException(
                                policyParser.getTokenLocation(),
                                policyName,
                                "field [" + ACTIONS_PARSEFIELD.getPreferredName() + "] already has value [" + action + "]"
                            );
                        }
                        actions |= FileEntitlement.READ_ACTION;
                    } else if (WRITE_ACTION.equals(action)) {
                        if ((actions & FileEntitlement.WRITE_ACTION) == FileEntitlement.WRITE_ACTION) {
                            throw newPolicyParserException(
                                policyParser.getTokenLocation(),
                                policyName,
                                "field [" + ACTIONS_PARSEFIELD.getPreferredName() + "] already has value [" + action + "]"
                            );
                        }
                        actions |= FileEntitlement.WRITE_ACTION;
                    } else {
                        throw newPolicyParserException(
                            policyParser.getTokenLocation(),
                            policyName,
                            "field [" + ACTIONS_PARSEFIELD.getPreferredName() + "] has unknown value [" + action + "]"
                        );
                    }
                }
            } else {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    scopeName,
                    "unexpected field [" + currentFieldName + "]"
                );
            }
        }
        return new FileEntitlement(path, actions);
    }
}
