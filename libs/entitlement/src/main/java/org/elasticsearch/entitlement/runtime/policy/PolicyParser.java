/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A parser to parse policy files for entitlements.
 */
public class PolicyParser {

    private static final Map<String, Class<?>> EXTERNAL_ENTITLEMENTS = Stream.of(FileEntitlement.class, CreateClassLoaderEntitlement.class)
        .collect(Collectors.toUnmodifiableMap(PolicyParser::getEntitlementTypeName, Function.identity()));

    protected final XContentParser policyParser;
    protected final String policyName;

    static String getEntitlementTypeName(Class<? extends Entitlement> entitlementClass) {
        var entitlementClassName = entitlementClass.getSimpleName();

        if (entitlementClassName.endsWith("Entitlement") == false) {
            throw new IllegalArgumentException(
                entitlementClassName + " is not a valid Entitlement class name. A valid class name must end with 'Entitlement'"
            );
        }

        var strippedClassName = entitlementClassName.substring(0, entitlementClassName.indexOf("Entitlement"));
        return Arrays.stream(strippedClassName.split("(?=\\p{Lu})"))
            .filter(Predicate.not(String::isEmpty))
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.joining("_"));
    }

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
            if (policyParser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw newPolicyParserException(scopeName, "expected array of <entitlement type>");
            }
            List<Entitlement> entitlements = new ArrayList<>();
            while (policyParser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (policyParser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    String entitlementType = policyParser.text();
                    Entitlement entitlement = parseEntitlement(scopeName, entitlementType);
                    entitlements.add(entitlement);
                } else if (policyParser.currentToken() == XContentParser.Token.START_OBJECT) {
                    if (policyParser.nextToken() != XContentParser.Token.FIELD_NAME) {
                        throw newPolicyParserException(scopeName, "expected object <entitlement type>");
                    }
                    String entitlementType = policyParser.currentName();
                    Entitlement entitlement = parseEntitlement(scopeName, entitlementType);
                    entitlements.add(entitlement);
                    if (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                        throw newPolicyParserException(scopeName, "expected closing object");
                    }
                } else {
                    throw newPolicyParserException(scopeName, "expected object <entitlement type>");
                }
            }
            return new Scope(scopeName, entitlements);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    protected Entitlement parseEntitlement(String scopeName, String entitlementType) throws IOException {
        Class<?> entitlementClass = EXTERNAL_ENTITLEMENTS.get(entitlementType);

        if (entitlementClass == null) {
            throw newPolicyParserException(scopeName, "unknown entitlement type [" + entitlementType + "]");
        }

        Constructor<?> entitlementConstructor = entitlementClass.getConstructors()[0];
        ExternalEntitlement entitlementMetadata = entitlementConstructor.getAnnotation(ExternalEntitlement.class);
        if (entitlementMetadata == null) {
            throw newPolicyParserException(scopeName, "unknown entitlement type [" + entitlementType + "]");
        }

        Class<?>[] parameterTypes = entitlementConstructor.getParameterTypes();
        String[] parametersNames = entitlementMetadata.parameterNames();

        if (parameterTypes.length != 0 || parametersNames.length != 0) {
            if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw newPolicyParserException(scopeName, entitlementType, "expected entitlement parameters");
            }
        }

        Map<String, Object> parsedValues = policyParser.map();

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
