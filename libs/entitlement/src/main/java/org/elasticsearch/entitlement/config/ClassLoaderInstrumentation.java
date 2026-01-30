/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRule;
import org.elasticsearch.entitlement.rules.EntitlementRules;
import org.elasticsearch.entitlement.rules.Policies;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.security.SecureClassLoader;
import java.util.function.Consumer;

public class ClassLoaderInstrumentation implements InstrumentationConfig {
    @Override
    public void init(Consumer<EntitlementRule> addRule) {
        EntitlementRules.on(addRule, ClassLoader.class)
            .protectedCtor()
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .protectedCtor(ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .protectedCtor(String.class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, URLClassLoader.class)
            .callingStatic(URLClassLoader::new, URL[].class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, URL[].class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, URL[].class, ClassLoader.class, URLStreamHandlerFactory.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class, URLStreamHandlerFactory.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::newInstance, URL[].class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .callingStatic(URLClassLoader::newInstance, URL[].class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled();

        EntitlementRules.on(addRule, SecureClassLoader.class)
            .protectedCtor()
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .protectedCtor(ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled()
            .protectedCtor(String.class, ClassLoader.class)
            .enforce(Policies::createClassLoader)
            .elseThrowNotEntitled();
    }
}
