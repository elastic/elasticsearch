/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.rule;

import java.util.function.UnaryOperator;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.sql.tree.Node;
import org.elasticsearch.xpack.sql.util.ReflectionUtils;

// Rule class that applies a transformation to a tree.
// In addition, performs type filtering so that a rule that works only on a type node can be skipped if necessary just based on the class signature. 

// Implementation detail:
// While lambdas are nice, actual node rules tend to be fairly large and are much better suited as full blown classes.
// In addition as they already embed their type information (all rule implementations end up with the generic information on them)
// this can be leveraged to perform the type filtering (as mentioned above).
// As a side note, getting the generic information from lambdas is very hacky and not portable (not without completely messing the JVM SM)

// apply - indicates how to apply the rule (transformUp/Down, transformExpressions..) on the target
// rule - contains the actual rule logic. 
public abstract class Rule<E extends T, T extends Node<T>> implements UnaryOperator<T> {

    protected Logger log = Loggers.getLogger(getClass());

    private final String name;
    private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());

    protected Rule() {
        this(null);
    }

    protected Rule(String name) {
        this.name = (name == null ? ReflectionUtils.ruleLikeNaming(getClass()) : name);
    }

    public Class<E> typeToken() {
        return typeToken;
    }

    public String name() {
        return name;
    }

    protected abstract T rule(E e);

    @Override
    public String toString() {
        return name();
    }
}