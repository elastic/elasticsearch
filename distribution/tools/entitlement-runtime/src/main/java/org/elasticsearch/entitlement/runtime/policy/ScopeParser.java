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

public abstract class ScopeParser {

    public static ParseField NAME_PARSEFIELD = new ParseField("name");
    public static ParseField ENTITLEMENTS_PARSEFIELD = new ParseField("entitlements");

    protected final String policyName;
    protected final XContentParser policyParser;

    protected final List<FileEntitlementParser> fileEntitlementParsers = new ArrayList<>();

    protected ScopeParser(String policyName, XContentParser policyParser) {
        this.policyName = policyName;
        this.policyParser = policyParser;
    }

    protected void parseScope() throws IOException {
        while (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (policyParser.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    "expected object [" + ModuleScopeParser.MODULE_PARSEFIELD.getPreferredName() + "]"
                );
            }
            String currentFieldName = policyParser.currentName();
            if (currentFieldName.equals(NAME_PARSEFIELD.getPreferredName())) {
                if (policyParser.nextToken() != XContentParser.Token.VALUE_STRING) {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        "expected value for field [" + NAME_PARSEFIELD.getPreferredName() + "]"
                    );
                }
                if (getScopeName() == null) {
                    setScopeName(policyParser.text());
                } else {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        "field ["
                            + NAME_PARSEFIELD.getPreferredName()
                            + "] has already been set; found multiple values"
                            + "['"
                            + getScopeName()
                            + "', '"
                            + policyParser.text()
                            + "']"
                    );
                }
            } else if (currentFieldName.equals(ENTITLEMENTS_PARSEFIELD.getPreferredName())) {
                if (policyParser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        getScopeName(),
                        "expected array of <entitlement type>"
                    );
                }
                parseEntitlements();
            } else {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    getScopeName(),
                    "unexpected field [" + currentFieldName + "]"
                );
            }
        }
    }

    protected void parseEntitlements() throws IOException {
        while (policyParser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (policyParser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    getScopeName(),
                    "expected object <entitlement type>"
                );
            }
            if (policyParser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    getScopeName(),
                    "expected object <entitlement type>"
                );
            }
            String currentFieldName = policyParser.currentName();
            if (currentFieldName.equals(FileEntitlementParser.FILE_PARSEFIELD.getPreferredName())) {
                FileEntitlementParser fileEntitlementParser = new FileEntitlementParser(policyName, getScopeName(), policyParser);
                fileEntitlementParser.parseEntitlement();
                fileEntitlementParsers.add(fileEntitlementParser);
            } else {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    getScopeName(),
                    "unexpected field [" + currentFieldName + "]"
                );
            }
            if (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw newPolicyParserException(policyParser.getTokenLocation(), policyName, getScopeName(), "expected closing object");
            }
        }
    }

    protected abstract void setScopeName(String scopeName);

    protected abstract String getScopeName();
}
