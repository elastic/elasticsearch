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
 * Simple wrapper for {@link org.apache.lucene.util.automaton.RegExp} that avoids throwing
 * StackOverflowErrors when working with regular expressions as this will crash an Elasticsearch node.
 * Instead, these StackOverflowErrors are turned into IllegalArgumentExceptions.
 */
@SuppressForbidden(reason = "catches StackOverflowError")
public class RegExp {

    private final org.apache.lucene.util.automaton.RegExp wrapped;

    public RegExp(String s) {
        try {
            wrapped = new org.apache.lucene.util.automaton.RegExp(s);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp", e);
        }
    }

    public RegExp(String s, int syntax_flags) {
        try {
            wrapped = new org.apache.lucene.util.automaton.RegExp(s, syntax_flags);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp", e);
        }
    }

    public RegExp(String s, int syntax_flags, int match_flags) {
        try {
            wrapped = new org.apache.lucene.util.automaton.RegExp(s, syntax_flags, match_flags);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp", e);
        }
    }

    public Automaton toAutomaton() {
        try {
            return wrapped.toAutomaton();
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp", e);
        }
    }

    public Automaton toAutomaton(int determinizeWorkLimit) {
        try {
            return wrapped.toAutomaton(determinizeWorkLimit);
        } catch (StackOverflowError e) {
            throw new IllegalArgumentException("failed to parse regexp", e);
        }
    }

    public String getOriginalString() {
        return wrapped.getOriginalString();
    }

    @Override
    public String toString() {
        // don't call wrapped.toString() to avoid StackOverflowError
        return getOriginalString();
    }
}
