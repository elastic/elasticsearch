/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.policy.entitlements.CreateClassLoaderEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.Entitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.InboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.LoadNativeLibrariesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.ManageThreadsEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.OutboundNetworkEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.SetHttpsConnectionPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteAllSystemPropertiesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.WriteSystemPropertiesEntitlement;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A parser to parse policy files for entitlements.
 */
public class PolicyParser {

    private static final Map<String, Class<? extends Entitlement>> EXTERNAL_ENTITLEMENT_CLASSES_BY_NAME = Stream.of(
        CreateClassLoaderEntitlement.class,
        FilesEntitlement.class,
        InboundNetworkEntitlement.class,
        LoadNativeLibrariesEntitlement.class,
        ManageThreadsEntitlement.class,
        OutboundNetworkEntitlement.class,
        SetHttpsConnectionPropertiesEntitlement.class,
        WriteAllSystemPropertiesEntitlement.class,
        WriteSystemPropertiesEntitlement.class
    ).collect(Collectors.toUnmodifiableMap(PolicyParser::buildEntitlementNameFromClass, Function.identity()));

    private static final Map<Class<? extends Entitlement>, String> EXTERNAL_ENTITLEMENT_NAMES_BY_CLASS =
        EXTERNAL_ENTITLEMENT_CLASSES_BY_NAME.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getValue, Map.Entry::getKey));

    protected final XContentParser policyParser;
    protected final String policyName;
    private final boolean isExternalPlugin;
    private final Map<String, Class<? extends Entitlement>> externalEntitlements;

    static String buildEntitlementNameFromClass(Class<? extends Entitlement> entitlementClass) {
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

    public static String getEntitlementName(Class<? extends Entitlement> entitlementClass) {
        return EXTERNAL_ENTITLEMENT_NAMES_BY_CLASS.get(entitlementClass);
    }

    public PolicyParser(InputStream inputStream, String policyName, boolean isExternalPlugin) throws IOException {
        this(inputStream, policyName, isExternalPlugin, EXTERNAL_ENTITLEMENT_CLASSES_BY_NAME);
    }

    // package private for tests
    PolicyParser(
        InputStream inputStream,
        String policyName,
        boolean isExternalPlugin,
        Map<String, Class<? extends Entitlement>> externalEntitlements
    ) throws IOException {
        this.policyParser = YamlXContent.yamlXContent.createParser(XContentParserConfiguration.EMPTY, Objects.requireNonNull(inputStream));
        this.policyName = policyName;
        this.isExternalPlugin = isExternalPlugin;
        this.externalEntitlements = externalEntitlements;
    }

    public VersionedPolicy parseVersionedPolicy() {
        Set<String> versions = Set.of();
        Policy policy = emptyPolicy();
        try {
            if (policyParser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw newPolicyParserException("expected object <versioned policy>");
            }

            while (policyParser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (policyParser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    if (policyParser.currentName().equals("versions")) {
                        versions = parseVersions();
                    } else if (policyParser.currentName().equals("policy")) {
                        policy = parsePolicy();
                    } else {
                        throw newPolicyParserException("expected either <version> or <policy> field");
                    }
                } else {
                    throw newPolicyParserException("expected either <version> or <policy> field");
                }
            }

            return new VersionedPolicy(policy, versions);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private Policy emptyPolicy() {
        return new Policy(policyName, List.of());
    }

    private Set<String> parseVersions() throws IOException {
        try {
            if (policyParser.nextToken() != XContentParser.Token.START_ARRAY) {
                throw newPolicyParserException("expected array of <versions>");
            }
            Set<String> versions = new HashSet<>();
            while (policyParser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (policyParser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    String version = policyParser.text();
                    versions.add(version);
                } else {
                    throw newPolicyParserException("expected <version>");
                }
            }
            return versions;
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
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
        XContentLocation startLocation = policyParser.getTokenLocation();
        Class<?> entitlementClass = externalEntitlements.get(entitlementType);

        if (entitlementClass == null) {
            throw newPolicyParserException(scopeName, "unknown entitlement type [" + entitlementType + "]");
        }

        Constructor<?> entitlementConstructor = null;
        Method entitlementMethod = null;
        ExternalEntitlement entitlementMetadata = null;
        for (var ctor : entitlementClass.getConstructors()) {
            var metadata = ctor.getAnnotation(ExternalEntitlement.class);
            if (metadata != null) {
                if (entitlementMetadata != null) {
                    throw new IllegalStateException(
                        "entitlement class ["
                            + entitlementClass.getName()
                            + "] has more than one constructor annotated with ExternalEntitlement"
                    );
                }
                entitlementConstructor = ctor;
                entitlementMetadata = metadata;
            }
        }
        for (var method : entitlementClass.getMethods()) {
            var metadata = method.getAnnotation(ExternalEntitlement.class);
            if (metadata != null) {
                if (Modifier.isStatic(method.getModifiers()) == false) {
                    throw new IllegalStateException(
                        "entitlement class [" + entitlementClass.getName() + "] has non-static method annotated with ExternalEntitlement"
                    );
                }
                if (entitlementMetadata != null) {
                    throw new IllegalStateException(
                        "entitlement class ["
                            + entitlementClass.getName()
                            + "] has more than one constructor and/or method annotated with ExternalEntitlement"
                    );
                }
                entitlementMethod = method;
                entitlementMetadata = metadata;
            }
        }

        if (entitlementMetadata == null) {
            throw newPolicyParserException(scopeName, "unknown entitlement type [" + entitlementType + "]");
        }

        if (entitlementMetadata.esModulesOnly() && isExternalPlugin) {
            throw newPolicyParserException("entitlement type [" + entitlementType + "] is allowed only on modules");
        }

        Class<?>[] parameterTypes = entitlementConstructor != null
            ? entitlementConstructor.getParameterTypes()
            : entitlementMethod.getParameterTypes();
        String[] parametersNames = entitlementMetadata.parameterNames();
        Object[] parameterValues = new Object[parameterTypes.length];

        if (parameterTypes.length != 0 || parametersNames.length != 0) {
            if (policyParser.nextToken() == XContentParser.Token.START_OBJECT) {
                Map<String, Object> parsedValues = policyParser.map();

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
                            "unexpected parameter type ["
                                + parameterType.getSimpleName()
                                + "] for entitlement parameter ["
                                + parameterName
                                + "]"
                        );
                    }
                    parameterValues[parameterIndex] = parameterValue;
                }
                if (parsedValues.isEmpty() == false) {
                    throw newPolicyParserException(scopeName, entitlementType, "extraneous entitlement parameter(s) " + parsedValues);
                }
            } else if (policyParser.currentToken() == XContentParser.Token.START_ARRAY) {
                List<Object> parsedValues = policyParser.list();
                parameterValues[0] = parsedValues;
            } else {
                throw newPolicyParserException(scopeName, entitlementType, "expected entitlement parameters");
            }
        }

        try {
            if (entitlementConstructor != null) {
                return (Entitlement) entitlementConstructor.newInstance(parameterValues);
            } else {
                return (Entitlement) entitlementMethod.invoke(null, parameterValues);
            }
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
            if (e.getCause() instanceof PolicyValidationException piae) {
                throw newPolicyParserException(startLocation, scopeName, entitlementType, piae);
            }
            throw new IllegalStateException("internal error", e);
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

    protected PolicyParserException newPolicyParserException(
        XContentLocation location,
        String scopeName,
        String entitlementType,
        PolicyValidationException cause
    ) {
        return PolicyParserException.newPolicyParserException(location, policyName, scopeName, entitlementType, cause);
    }
}
