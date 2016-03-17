/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.privilege;

import dk.brics.automaton.Automaton;
import org.elasticsearch.common.Strings;
import org.elasticsearch.shield.support.Automatons;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Predicate;

import static org.elasticsearch.shield.support.Automatons.minusAndDeterminize;
import static org.elasticsearch.shield.support.Automatons.patterns;

/**
 *
 */
public class ClusterPrivilege extends AbstractAutomatonPrivilege<ClusterPrivilege> {

    // shared automatons
    private static final Automaton MANAGE_SECURITY_AUTOMATON = patterns("cluster:admin/xpack/security/*");
    private static final Automaton MONITOR_AUTOMATON = patterns("cluster:monitor/*");
    private static final Automaton ALL_CLUSTER_AUTOMATON = patterns("cluster:*", "indices:admin/template/*");
    private static final Automaton MANAGE_AUTOMATON = minusAndDeterminize(ALL_CLUSTER_AUTOMATON, MANAGE_SECURITY_AUTOMATON);
    private static final Automaton TRANSPORT_CLIENT_AUTOMATON = patterns("cluster:monitor/nodes/liveness", "cluster:monitor/state");
    private static final Automaton MANAGE_IDX_TEMPLATE_AUTOMATON = patterns("indices:admin/template/*");

    public static final ClusterPrivilege NONE =                  new ClusterPrivilege(Name.NONE,                Automatons.EMPTY);
    public static final ClusterPrivilege ALL =                   new ClusterPrivilege(Name.ALL,                 ALL_CLUSTER_AUTOMATON);
    public static final ClusterPrivilege MONITOR =               new ClusterPrivilege("monitor",                MONITOR_AUTOMATON);
    public static final ClusterPrivilege MANAGE =                new ClusterPrivilege("manage",                 MANAGE_AUTOMATON);
    public static final ClusterPrivilege MANAGE_IDX_TEMPLATES =
            new ClusterPrivilege("manage_index_templates", MANAGE_IDX_TEMPLATE_AUTOMATON);
    public static final ClusterPrivilege TRANSPORT_CLIENT =      new ClusterPrivilege("transport_client",       TRANSPORT_CLIENT_AUTOMATON);
    public static final ClusterPrivilege MANAGE_SECURITY =       new ClusterPrivilege("manage_security",        MANAGE_SECURITY_AUTOMATON);

    public final static Predicate<String> ACTION_MATCHER = ClusterPrivilege.ALL.predicate();

    private static final Set<ClusterPrivilege> values = new CopyOnWriteArraySet<>();

    static {
        values.add(NONE);
        values.add(ALL);
        values.add(MONITOR);
        values.add(MANAGE);
        values.add(MANAGE_IDX_TEMPLATES);
        values.add(TRANSPORT_CLIENT);
        values.add(MANAGE_SECURITY);
    }

    static Set<ClusterPrivilege> values() {
        return values;
    }

    private static final ConcurrentHashMap<Name, ClusterPrivilege> cache = new ConcurrentHashMap<>();

    private ClusterPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    private ClusterPrivilege(String name, Automaton automaton) {
        super(new Name(name), automaton);
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
