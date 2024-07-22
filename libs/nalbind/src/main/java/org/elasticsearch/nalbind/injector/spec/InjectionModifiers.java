/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nalbind.injector.spec;

import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.EnumSet.noneOf;

public enum InjectionModifiers {
    /**
     * Indicates a proxy object is unacceptable; must inject the object itself.
     * This creates a hard creation-order constraint between the objects.
     * Non-list parameters are always <code>ACTUAL</code> regardless of whether
     * they use the {@link org.elasticsearch.nalbind.api.Actual @Actual} annotation.
     *
     * @see org.elasticsearch.nalbind.api.Actual
     */
    ACTUAL,

    /**
     * Indicates we should inject a {@link List} of all instances of some class, instead of a single object.
     * Allows for a one-to-many relationship.
     * Objects are listed in creation order.
     */
    LIST,
    ;

    public static final Set<InjectionModifiers> NONE = unmodifiableSet(noneOf(InjectionModifiers.class));
}
