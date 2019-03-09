/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;

public class Privilege {

    public static final Privilege NONE = new Privilege(Collections.singleton("none"), Automatons.EMPTY);
    public static final Privilege ALL = new Privilege(Collections.singleton("all"), Automatons.MATCH_ALL);

    protected final Set<String> name;
    protected final Automaton automaton;
    protected final Predicate<String> predicate;

    public Privilege(String name, String... patterns) {
        this(Collections.singleton(name), patterns);
    }

    public Privilege(Set<String> name, String... patterns) {
        this(name, patterns(patterns));
    }

    public Privilege(Set<String> name, Automaton automaton) {
        this.name = name;
        this.automaton = automaton;
        this.predicate = Automatons.predicate(automaton);
    }

    public Set<String> name() {
        return name;
    }

    public Predicate<String> predicate() {
        return predicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Privilege privilege = (Privilege) o;

        if (name != null ? !name.equals(privilege.name) : privilege.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    static String actionToPattern(String text) {
        return text + "*";
    }

    @Override
    public String toString() {
        return name.toString();
    }

    public Automaton getAutomaton() {
        return automaton;
    }
}
