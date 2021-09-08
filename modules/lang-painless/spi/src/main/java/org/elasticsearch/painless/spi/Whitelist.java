/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Whitelist contains data structures designed to be used to generate a whitelist of Java classes,
 * constructors, methods, and fields that can be used within a Painless script at both compile-time
 * and run-time.
 *
 * A whitelist consists of several pieces with {@link WhitelistClass}s as the top level. Each
 * {@link WhitelistClass} will contain zero-to-many {@link WhitelistConstructor}s, {@link WhitelistMethod}s, and
 * {@link WhitelistField}s which are what will be available with a Painless script.  See each individual
 * whitelist object for more detail.
 */
public final class Whitelist {

    /** The {@link ClassLoader} used to look up the whitelisted Java classes, constructors, methods, and fields. */
    public final ClassLoader classLoader;

    /** The {@link List} of all the whitelisted Painless classes. */
    public final List<WhitelistClass> whitelistClasses;

    /** The {@link List} of all the whitelisted static Painless methods. */
    public final List<WhitelistMethod> whitelistImportedMethods;

    /** The {@link List} of all the whitelisted Painless class bindings. */
    public final List<WhitelistClassBinding> whitelistClassBindings;

    /** The {@link List} of all the whitelisted Painless instance bindings. */
    public final List<WhitelistInstanceBinding> whitelistInstanceBindings;

    /** Standard constructor. All values must be not {@code null}. */
    public Whitelist(ClassLoader classLoader, List<WhitelistClass> whitelistClasses, List<WhitelistMethod> whitelistImportedMethods,
            List<WhitelistClassBinding> whitelistClassBindings, List<WhitelistInstanceBinding> whitelistInstanceBindings) {

        this.classLoader = Objects.requireNonNull(classLoader);
        this.whitelistClasses = Collections.unmodifiableList(Objects.requireNonNull(whitelistClasses));
        this.whitelistImportedMethods = Collections.unmodifiableList(Objects.requireNonNull(whitelistImportedMethods));
        this.whitelistClassBindings = Collections.unmodifiableList(Objects.requireNonNull(whitelistClassBindings));
        this.whitelistInstanceBindings = Collections.unmodifiableList(Objects.requireNonNull(whitelistInstanceBindings));
    }
}
