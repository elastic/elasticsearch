/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.privilege;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;
import org.elasticsearch.common.Strings;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Predicate;

/**
 *
 */
public class ClusterPrivilege extends AbstractAutomatonPrivilege<ClusterPrivilege> {

    public static final ClusterPrivilege NONE = new ClusterPrivilege(Name.NONE, BasicAutomata.makeEmpty());
    public static final ClusterPrivilege ALL = new ClusterPrivilege(Name.ALL, "cluster:*", "indices:admin/template/*");
    public static final ClusterPrivilege MONITOR = new ClusterPrivilege("monitor", "cluster:monitor/*");
    public static final ClusterPrivilege MANAGE_SHIELD = new ClusterPrivilege("manage_shield", "cluster:admin/shield/*");

    public final static Predicate<String> ACTION_MATCHER = ClusterPrivilege.ALL.predicate();

    private static final Set<ClusterPrivilege> values = new CopyOnWriteArraySet<>();

    static {
        values.add(NONE);
        values.add(ALL);
        values.add(MONITOR);
        values.add(MANAGE_SHIELD);
    }

    static Set<ClusterPrivilege> values() {
        return values;
    }

    private static final ConcurrentHashMap<Name, ClusterPrivilege> cache = new ConcurrentHashMap<>();

    private ClusterPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    private ClusterPrivilege(Name name, String... patterns) {
        super(name, patterns);
    }

    private ClusterPrivilege(Name name, Automaton automaton) {
        super(name, automaton);
    }

    public static void addCustom(String name, String... actionPatterns) {
        for (String pattern : actionPatterns) {
            if (!ClusterPrivilege.ACTION_MATCHER.test(pattern)) {
                throw new IllegalArgumentException("cannot register custom cluster privilege [" + name + "]. " +
                        "cluster action must follow the 'cluster:*' format");
            }
        }
        ClusterPrivilege custom = new ClusterPrivilege(name, actionPatterns);
        if (values.contains(custom)) {
            throw new IllegalArgumentException("cannot register custom cluster privilege [" + name + "] as it already exists.");
        }
        values.add(custom);
    }

    @Override
    protected ClusterPrivilege create(Name name, Automaton automaton) {
        return new ClusterPrivilege(name, automaton);
    }

    @Override
    protected ClusterPrivilege none() {
        return NONE;
    }

    public static ClusterPrivilege action(String action) {
        String pattern = actionToPattern(action);
        return new ClusterPrivilege(action, pattern);
    }

    public static ClusterPrivilege get(Name name) {
        return cache.computeIfAbsent(name, (theName) -> {
            ClusterPrivilege cluster = NONE;
            for (String part : theName.parts) {
                cluster = cluster == NONE ? resolve(part) : cluster.plus(resolve(part));
            }
            return cluster;
        });
    }

    private static ClusterPrivilege resolve(String name) {
        name = name.toLowerCase(Locale.ROOT);
        if (ACTION_MATCHER.test(name)) {
            return action(name);
        }
        for (ClusterPrivilege cluster : values) {
            if (name.equals(cluster.name.toString())) {
                return cluster;
            }
        }
        throw new IllegalArgumentException("unknown cluster privilege [" + name + "]. a privilege must be either " +
                "one of the predefined fixed cluster privileges [" + Strings.collectionToCommaDelimitedString(values) +
                "] or a pattern over one of the available cluster actions");
    }
}
