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
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.entitlement.runtime.policy.PolicyParserException.newPolicyParserException;

public class PolicyBuilder {

    public static ParseField POLICY_PARSEFIELD = new ParseField("policy");

    public final String policyName;
    public final XContentParser policyParser;

    public final List<ModuleScopeBuilder> policyModules = new ArrayList<>();

    public PolicyBuilder(String policyName, InputStream inputStream) {
        this.policyName = policyName;

        try {
            this.policyParser = YamlXContent.yamlXContent.createParser(
                XContentParserConfiguration.EMPTY,
                Objects.requireNonNull(inputStream)
            );
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    public void buildPolicy() {
        try {
            if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    "expected object [" + POLICY_PARSEFIELD.getPreferredName() + "]"
                );
            }
            if (policyParser.nextToken() != XContentParser.Token.FIELD_NAME
                || policyParser.currentName().equals(POLICY_PARSEFIELD.getPreferredName()) == false) {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    "expected object [" + POLICY_PARSEFIELD.getPreferredName() + "]"
                );
            }
            if (policyParser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw newPolicyParserException(
                    policyParser.getTokenLocation(),
                    policyName,
                    "expected array of [" + ModuleScopeBuilder.MODULE_PARSEFIELD.getPreferredName() + "]"
                );
            }
            while (policyParser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (policyParser.currentToken() != XContentParser.Token.START_OBJECT) {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        "expected object [" + ModuleScopeBuilder.MODULE_PARSEFIELD.getPreferredName() + "]"
                    );
                }
                if (policyParser.nextToken() != XContentParser.Token.FIELD_NAME
                    || policyParser.currentName().equals(ModuleScopeBuilder.MODULE_PARSEFIELD.getPreferredName()) == false) {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        "expected object [" + ModuleScopeBuilder.MODULE_PARSEFIELD.getPreferredName() + "]"
                    );
                }
                if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw newPolicyParserException(
                        policyParser.getTokenLocation(),
                        policyName,
                        "expected object [" + ModuleScopeBuilder.MODULE_PARSEFIELD.getPreferredName() + "]"
                    );
                }
                ModuleScopeBuilder moduleScopeBuilder = new ModuleScopeBuilder(policyName, policyParser);
                moduleScopeBuilder.parseScope();
                policyModules.add(moduleScopeBuilder);
                if (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                    throw newPolicyParserException(policyParser.getTokenLocation(), policyName, "expected closing object");
                }
            }
            if (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw newPolicyParserException(policyParser.getTokenLocation(), policyName, "expected closing object");
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

}
