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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.entitlement.runtime.policy.PolicyParserException.newPolicyParserException;

/**
 * A parser to parse policy files for entitlements.
 */
public class PolicyParser {

    protected static final ParseField ENTITLEMENTS_PARSEFIELD = new ParseField("entitlements");

    protected static final String entitlementPackageName = Entitlement.class.getPackage().getName();

    protected final XContentParser policyParser;
    protected final String policyName;

    public PolicyParser(InputStream inputStream, String policyName) throws IOException {
        this.policyParser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, Objects.requireNonNull(inputStream));
        this.policyName = policyName;
    }

    public Policy parsePolicy() {
        try {
            if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw newPolicyParserException("expected object <scope name>");
            }
            List<Scope> scopes = new ArrayList<>();
            while (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (policyParser.currentToken() != XContentParser.Token.FIELD_NAME) {
                    throw newPolicyParserException("expected object <scope name>");
                }
                String scopeName = policyParser.currentName();
                Scope scope = parseScope(scopeName);
                scopes.add(scope);
            }
            return new Policy(policyName, scopes);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    protected Scope parseScope(String scopeName) throws IOException {
        try {
            if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw newPolicyParserException(scopeName, "expected object [" + ENTITLEMENTS_PARSEFIELD.getPreferredName() + "]");
            }
            if (policyParser.nextToken() != XContentParser.Token.FIELD_NAME
                || policyParser.currentName().equals(ENTITLEMENTS_PARSEFIELD.getPreferredName()) == false) {
                throw newPolicyParserException(scopeName, "expected object [" + ENTITLEMENTS_PARSEFIELD.getPreferredName() + "]");
            }
            if (policyParser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw newPolicyParserException(scopeName, "expected array of <entitlement type>");
            }
            List<Entitlement> entitlements = new ArrayList<>();
            while (policyParser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (policyParser.currentToken() != XContentParser.Token.START_OBJECT) {
                    throw newPolicyParserException(scopeName, "expected object <entitlement type>");
                }
                if (policyParser.nextToken() != XContentParser.Token.FIELD_NAME) {
                    throw newPolicyParserException(scopeName, "expected object <entitlement type>");
                }
                String entitlementType = policyParser.currentName();
                Entitlement entitlement = parseEntitlement(scopeName, entitlementType);
                entitlements.add(entitlement);
                if (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                    throw newPolicyParserException(scopeName, "expected closing object");
                }
            }
            if (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw newPolicyParserException(scopeName, "expected closing object");
            }
            return new Scope(scopeName, entitlements);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    protected Entitlement parseEntitlement(String scopeName, String entitlementType) throws IOException {
        Class<?> entitlementClass;
        try {
            entitlementClass = Class.forName(
                entitlementPackageName
                    + "."
                    + Character.toUpperCase(entitlementType.charAt(0))
                    + entitlementType.substring(1)
                    + "Entitlement"
            );
        } catch (ClassNotFoundException cnfe) {
            throw newPolicyParserException(scopeName, "unknown entitlement type [" + entitlementType + "]");
        }
        if (Entitlement.class.isAssignableFrom(entitlementClass) == false) {
            throw newPolicyParserException(scopeName, "unknown entitlement type [" + entitlementType + "]");
        }
        Constructor<?> entitlementConstructor = entitlementClass.getConstructors()[0];
        ExternalEntitlement entitlementMetadata = entitlementConstructor.getAnnotation(ExternalEntitlement.class);
        if (entitlementMetadata == null) {
            throw newPolicyParserException(scopeName, "unknown entitlement type [" + entitlementType + "]");
        }

        if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw newPolicyParserException(scopeName, entitlementType, "expected entitlement parameters");
        }
        Map<String, Object> parsedValues = policyParser.map();

        Class<?>[] parameterTypes = entitlementConstructor.getParameterTypes();
        String[] parametersNames = entitlementMetadata.parameterNames();
        Object[] parameterValues = new Object[parameterTypes.length];
        for (int parameterIndex = 0; parameterIndex < parameterTypes.length; ++parameterIndex) {
            String parameterName = parametersNames[parameterIndex];
            Object parameterValue = parsedValues.remove(parameterName);
            if (parameterValue == null) {
                throw newPolicyParserException(scopeName, entitlementType, "missing entitlement parameter [" + parameterName + "]");
            }
            Class<?> parameterType = parameterTypes[parameterIndex];
            if (parameterType.isAssignableFrom(parameterValue.getClass()) == false) {
                throw newPolicyParserException(
                    scopeName,
                    entitlementType,
                    "unexpected parameter type [" + parameterType.getSimpleName() + "] for entitlement parameter [" + parameterName + "]"
                );
            }
            parameterValues[parameterIndex] = parameterValue;
        }
        if (parsedValues.isEmpty() == false) {
            throw newPolicyParserException(scopeName, entitlementType, "extraneous entitlement parameter(s) " + parsedValues);
        }

        try {
            return (Entitlement) entitlementConstructor.newInstance(parameterValues);
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("internal error");
        }
    }

    protected PolicyParserException newPolicyParserException(String message) {
        return PolicyParserException.newPolicyParserException(policyParser.getTokenLocation(), policyName, message);
    }

    protected PolicyParserException newPolicyParserException(String scopeName, String message) {
        return PolicyParserException.newPolicyParserException(policyParser.getTokenLocation(), policyName, scopeName, message);
    }

    protected PolicyParserException newPolicyParserException(String scopeName, String entitlementType, String message) {
        return PolicyParserException.newPolicyParserException(
            policyParser.getTokenLocation(),
            policyName,
            scopeName,
            entitlementType,
            message
        );
    }
}
