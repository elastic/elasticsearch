/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.entitlement.runtime.policy.PolicyParserException.newPolicyParserException;

public class FileEntitlementParser extends EntitlementParser {

    public static ParseField FILE_PARSEFIELD = new ParseField("file");
    public static ParseField PATH_PARSEFIELD = new ParseField("path");
    public static ParseField ACTIONS_PARSEFIELD = new ParseField("actions");

    protected String path;
    protected final List<String> actions = new ArrayList<>();

    protected FileEntitlementParser(String policyName, String scopeName, XContentParser policyParser) {
        super(policyName, scopeName, policyParser);
    }

    protected void parseEntitlement() throws IOException {
        if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw newPolicyParserException(
                policyParser.getTokenLocation(),
                policyName,
                scopeName,
                "expected object [" + FILE_PARSEFIELD.getPreferredName() + "]"
            );
        }
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
                    if (actions.contains(action) == false) {
                        actions.add(action);
                    } else {
                        throw newPolicyParserException(
                            policyParser.getTokenLocation(),
                            policyName,
                            "field [" + ACTIONS_PARSEFIELD.getPreferredName() + "] already has value [" + policyParser.text() + "]"
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
    }
}
