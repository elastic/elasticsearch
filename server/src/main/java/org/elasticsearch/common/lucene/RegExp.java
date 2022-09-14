/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.core.SuppressForbidden;

/**
 * Simple wrapper for {@link org.apache.lucene.util.automaton.RegExp} that
 * avoids throwing {@link StackOverflowError}s when working with regular
 * expressions as this will crash an Elasticsearch node. Instead, these
 * {@linkplain StackOverflowError}s are turned into
 * {@link IllegalArgumentException}s which elasticsearch returns as
 * a http 400.
 */
public class RegExp {

    @SuppressForbidden(reason = "this class is the trusted wrapper")
    private final org.apache.lucene.util.automaton.RegExp wrapped;

    @SuppressForbidden(reason = "catches StackOverflowError")
    public RegExp(String s) {
        try {
            wrapped = new org.apache.lucene.util.automaton.RegExp(s);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp due to stack overflow: " + s);
        }
    }

    @SuppressForbidden(reason = "catches StackOverflowError")
    public RegExp(String s, int syntax_flags) {
        try {
            wrapped = new org.apache.lucene.util.automaton.RegExp(s, syntax_flags);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp due to stack overflow: " + s);
        }
    }

    @SuppressForbidden(reason = "catches StackOverflowError")
    public RegExp(String s, int syntax_flags, int match_flags) {
        try {
            wrapped = new org.apache.lucene.util.automaton.RegExp(s, syntax_flags, match_flags);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp due to stack overflow: " + s);
        }
    }

    @SuppressForbidden(reason = "we are the trusted wrapper")
    private RegExp(org.apache.lucene.util.automaton.RegExp wrapped) {
        this.wrapped = wrapped;
    }

    @SuppressForbidden(reason = "catches StackOverflowError")
    public Automaton toAutomaton() {
        try {
            return wrapped.toAutomaton();
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp due to stack overflow: " + this);
        }
    }

    @SuppressForbidden(reason = "catches StackOverflowError")
    public Automaton toAutomaton(int determinizeWorkLimit) {
        try {
            return wrapped.toAutomaton(determinizeWorkLimit);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp due to stack overflow: " + this);
        }
    }

    @SuppressForbidden(reason = "his class is the trusted wrapper")
    public String getOriginalString() {
        return wrapped.getOriginalString();
    }

    @Override
    public String toString() {
        // don't call wrapped.toString() to avoid StackOverflowError
        return getOriginalString();
    }

    /**
     * The type of expression.
     */
    @SuppressForbidden(reason = "we are the trusted wrapper")
    public org.apache.lucene.util.automaton.RegExp.Kind kind() {
        return wrapped.kind;
    }

    /**
     * Child expressions held by a container type expression.
     */
    @SuppressForbidden(reason = "we are the trusted wrapper")
    public RegExp exp1() {
        return new RegExp(wrapped.exp1);
    }

    /**
     * Child expressions held by a container type expression.
     */
    @SuppressForbidden(reason = "we are the trusted wrapper")
    public RegExp exp2() {
        return new RegExp(wrapped.exp2);
    }

    /**
     * Limits for repeatable type expressions.
     */
    @SuppressForbidden(reason = "we are the trusted wrapper")
    public int min() {
        return wrapped.min;
    }

    /**
     * String expression.
     */
    @SuppressForbidden(reason = "we are the trusted wrapper")
    public String s() {
        return wrapped.s;
    }

    /**
     * Character expression.
     */
    @SuppressForbidden(reason = "we are the trusted wrapper")
    public int c() {
        return wrapped.c;
    }
}
