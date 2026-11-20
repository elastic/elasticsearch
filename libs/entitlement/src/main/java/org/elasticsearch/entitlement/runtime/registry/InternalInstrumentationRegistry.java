/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.registry;

import org.elasticsearch.entitlement.bridge.InstrumentationRegistry;
import org.elasticsearch.entitlement.instrumentation.MethodSignature;
import org.elasticsearch.entitlement.rules.EntitlementRule;

import java.util.Map;

/**
 * Internal extension of {@link InstrumentationRegistry} used during entitlement initialization to
 * build up and query the set of instrumented methods. The bridge {@link InstrumentationRegistry}
 * handles runtime check dispatch; this interface adds the ability to register rules at startup
 * and retrieve the full rule map for use by the instrumenter.
 */
public interface InternalInstrumentationRegistry extends InstrumentationRegistry {

    /**
     * Returns all registered instrumentation rules, grouped by internal class name.
     * The outer map is keyed by the class's internal name (e.g. {@code "java/net/Socket"}),
     * and each inner map is keyed by {@link MethodSignature} mapping to the
     * {@link InstrumentationInfo} for that method.
     */
    Map<String, Map<MethodSignature, InstrumentationInfo>> getInstrumentedMethods();

    /**
     * Registers an entitlement rule for instrumentation. Rules are indexed by the
     * {@link EntitlementRule#methodKey() method key}'s class name and method signature.
     *
     * @throws IllegalStateException if a rule has already been registered for the same method
     */
    void registerRule(EntitlementRule rule);

    /**
     * Validates that no two types in the same hierarchy define a rule for the same non-constructor
     * method signature. Must be called after all rules have been registered and before
     * instrumentation begins.
     * <p>
     * This ensures that inherited rule resolution is always unambiguous: for any method on any
     * class, there is at most one inherited rule in the supertype hierarchy. Constructors
     * ({@code <init>}) are exempt because they are never inherited.
     *
     * @throws IllegalStateException if overlapping hierarchy rules are detected
     */
    void validate();
}
