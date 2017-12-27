/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.mapper.expressiondsl;

import java.util.Map;
import java.util.function.Predicate;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

/**
 * Implementations of this interface represent an expression over a simple object that resolves to
 * a boolean value. The "simple object" is implemented as a (flattened) {@link Map}.
 */
public interface RoleMapperExpression extends ToXContentObject, NamedWriteable {

    /**
     * Determines whether this expression matches against the provided object.
     */
    boolean match(Map<String, Object> object);

    /**
     * Adapt this expression to a standard {@link Predicate}
     */
    default Predicate<Map<String, Object>> asPredicate() {
        return this::match;
    }

    /**
     * Creates an <em>inverted</em> predicate that can test whether an expression matches
     * a fixed object. Its purpose is for cases where there is a {@link java.util.stream.Stream} of
     * expressions, that need to be filtered against a single map.
     */
    static Predicate<RoleMapperExpression> predicate(Map<String, Object> map) {
        return expr -> expr.match(map);
    }

}
