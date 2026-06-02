/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.rules.EntitlementRulesBuilder;
import org.elasticsearch.entitlement.rules.Policies;
import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;
import java.security.SecureClassLoader;

public class ClassLoaderInstrumentation implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        EntitlementRulesBuilder builder = new EntitlementRulesBuilder(registry);

        builder.on(ClassLoader.class, rule -> {
            rule.protectedCtor().enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.protectedCtor(ClassLoader.class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.protectedCtor(String.class, ClassLoader.class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
        });

        builder.on(URLClassLoader.class, rule -> {
            rule.callingStatic(URLClassLoader::new, URL[].class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, URL[].class, ClassLoader.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, URL[].class, ClassLoader.class, URLStreamHandlerFactory.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::new, String.class, URL[].class, ClassLoader.class, URLStreamHandlerFactory.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::newInstance, URL[].class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.callingStatic(URLClassLoader::newInstance, URL[].class, ClassLoader.class)
                .enforce(Policies::createClassLoader)
                .elseThrowNotEntitled();
        });

        builder.on(SecureClassLoader.class, rule -> {
            rule.protectedCtor().enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.protectedCtor(ClassLoader.class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
            rule.protectedCtor(String.class, ClassLoader.class).enforce(Policies::createClassLoader).elseThrowNotEntitled();
        });

        builder.on(MethodHandles.Lookup.class, rule -> {
            rule.calling(MethodHandles.Lookup::defineClass, byte[].class)
                .enforce(Policies::createClassLoader)
                .elseThrow(e -> new IllegalAccessException(e.getMessage()));

            // On Java 17, InnerClassLambdaMetafactory routes every lambda creation through
            // the public defineHiddenClass and defineHiddenClassWithClassData methods. Applying
            // create_class_loader checks to these methods on Java 17 would block ordinary lambda
            // expressions in any component that lacks the entitlement. On Java 21+ the lambda
            // factory bypasses the public Java method via a native helper, so the checks can
            // safely apply. We therefore skip these rules on Java 17.
            if (Runtime.version().feature() >= 21) {
                rule.calling(MethodHandles.Lookup::defineHiddenClass, byte[].class, Boolean.class, MethodHandles.Lookup.ClassOption[].class)
                    .enforce(Policies::createClassLoader)
                    .elseThrow(e -> new IllegalAccessException(e.getMessage()));
                rule.calling(
                    MethodHandles.Lookup::defineHiddenClassWithClassData,
                    byte[].class,
                    Object.class,
                    Boolean.class,
                    MethodHandles.Lookup.ClassOption[].class
                ).enforce(Policies::createClassLoader).elseThrow(e -> new IllegalAccessException(e.getMessage()));
            }
        });
    }
}
