/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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

        return Objects.equals(name, privilege.name);
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

    /**
     * Sorts the map of privileges from least-privilege to most-privilege
     */
    static <T extends Privilege> SortedMap<String, T> sortByAccessLevel(Map<String, T> privileges) {
        // How many other privileges is this privilege a subset of. Those with a higher count are considered to be a lower privilege
        final Map<String, Long> subsetCount = Maps.newMapWithExpectedSize(privileges.size());
        privileges.forEach(
            (name, priv) -> subsetCount.put(
                name,
                privileges.values().stream().filter(p2 -> p2 != priv && Operations.subsetOf(priv.automaton, p2.automaton)).count()
            )
        );

        final Comparator<String> compare = Comparator.<String>comparingLong(key -> subsetCount.getOrDefault(key, 0L))
            .reversed()
            .thenComparing(Comparator.naturalOrder());
        final TreeMap<String, T> tree = new TreeMap<>(compare);
        tree.putAll(privileges);
        return Collections.unmodifiableSortedMap(tree);
    }
}
