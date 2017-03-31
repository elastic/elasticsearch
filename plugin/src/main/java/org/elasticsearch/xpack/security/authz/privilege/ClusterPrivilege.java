/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.privilege;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xpack.security.support.Automatons;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.security.support.Automatons.minusAndMinimize;
import static org.elasticsearch.xpack.security.support.Automatons.patterns;

public final class ClusterPrivilege extends Privilege {

    // shared automatons
    private static final Automaton MANAGE_SECURITY_AUTOMATON = patterns("cluster:admin/xpack/security/*");
    private static final Automaton MONITOR_AUTOMATON = patterns("cluster:monitor/*");
    private static final Automaton MONITOR_ML_AUTOMATON = patterns("cluster:monitor/xpack/ml/*");
    private static final Automaton ALL_CLUSTER_AUTOMATON = patterns("cluster:*", "indices:admin/template/*");
    private static final Automaton MANAGE_AUTOMATON = minusAndMinimize(ALL_CLUSTER_AUTOMATON, MANAGE_SECURITY_AUTOMATON);
    private static final Automaton MANAGE_ML_AUTOMATON = patterns("cluster:admin/xpack/ml/*", "cluster:monitor/xpack/ml/*");
    private static final Automaton TRANSPORT_CLIENT_AUTOMATON = patterns("cluster:monitor/nodes/liveness", "cluster:monitor/state");
    private static final Automaton MANAGE_IDX_TEMPLATE_AUTOMATON = patterns("indices:admin/template/*");
    private static final Automaton MANAGE_INGEST_PIPELINE_AUTOMATON = patterns("cluster:admin/ingest/pipeline/*");

    public static final ClusterPrivilege NONE =                  new ClusterPrivilege("none",                Automatons.EMPTY);
    public static final ClusterPrivilege ALL =                   new ClusterPrivilege("all",                 ALL_CLUSTER_AUTOMATON);
    public static final ClusterPrivilege MONITOR =               new ClusterPrivilege("monitor",             MONITOR_AUTOMATON);
    public static final ClusterPrivilege MONITOR_ML =            new ClusterPrivilege("monitor_ml",          MONITOR_ML_AUTOMATON);
    public static final ClusterPrivilege MANAGE =                new ClusterPrivilege("manage",              MANAGE_AUTOMATON);
    public static final ClusterPrivilege MANAGE_ML =             new ClusterPrivilege("manage_ml",           MANAGE_ML_AUTOMATON);
    public static final ClusterPrivilege MANAGE_IDX_TEMPLATES =
            new ClusterPrivilege("manage_index_templates", MANAGE_IDX_TEMPLATE_AUTOMATON);
    public static final ClusterPrivilege MANAGE_INGEST_PIPELINES =
            new ClusterPrivilege("manage_ingest_pipelines", MANAGE_INGEST_PIPELINE_AUTOMATON);
    public static final ClusterPrivilege TRANSPORT_CLIENT =      new ClusterPrivilege("transport_client",    TRANSPORT_CLIENT_AUTOMATON);
    public static final ClusterPrivilege MANAGE_SECURITY =       new ClusterPrivilege("manage_security",     MANAGE_SECURITY_AUTOMATON);
    public static final ClusterPrivilege MANAGE_PIPELINE =       new ClusterPrivilege("manage_pipeline", "cluster:admin/ingest/pipeline/*");

    public static final Predicate<String> ACTION_MATCHER = ClusterPrivilege.ALL.predicate();

    private static final Map<String, ClusterPrivilege> VALUES = MapBuilder.<String, ClusterPrivilege>newMapBuilder()
            .put("none", NONE)
            .put("all", ALL)
            .put("monitor", MONITOR)
            .put("monitor_ml", MONITOR_ML)
            .put("manage", MANAGE)
            .put("manage_ml", MANAGE_ML)
            .put("manage_index_templates", MANAGE_IDX_TEMPLATES)
            .put("manage_ingest_pipelines", MANAGE_INGEST_PIPELINES)
            .put("transport_client", TRANSPORT_CLIENT)
            .put("manage_security", MANAGE_SECURITY)
            .put("manage_pipeline", MANAGE_PIPELINE)
            .immutableMap();

    private static final ConcurrentHashMap<Set<String>, ClusterPrivilege> CACHE = new ConcurrentHashMap<>();

    private ClusterPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    private ClusterPrivilege(String name, Automaton automaton) {
        super(Collections.singleton(name), automaton);
    }

    private ClusterPrivilege(Set<String> name, Automaton automaton) {
        super(name, automaton);
    }

    public static ClusterPrivilege get(Set<String> name) {
        if (name == null || name.isEmpty()) {
            return NONE;
        }
        return CACHE.computeIfAbsent(name, ClusterPrivilege::resolve);
    }

    private static ClusterPrivilege resolve(Set<String> name) {
        final int size = name.size();
        if (size == 0) {
            throw new IllegalArgumentException("empty set should not be used");
        }

        Set<String> actions = new HashSet<>();
        Set<Automaton> automata = new HashSet<>();
        for (String part : name) {
            part = part.toLowerCase(Locale.ROOT);
            if (ACTION_MATCHER.test(part)) {
                actions.add(actionToPattern(part));
            } else {
                ClusterPrivilege privilege = VALUES.get(part);
                if (privilege != null && size == 1) {
                    return privilege;
                } else if (privilege != null) {
                    automata.add(privilege.automaton);
                } else {
                    throw new IllegalArgumentException("unknown cluster privilege [" + name + "]. a privilege must be either " +
                            "one of the predefined fixed cluster privileges [" +
                            Strings.collectionToCommaDelimitedString(VALUES.entrySet()) + "] or a pattern over one of the available " +
                            "cluster actions");
                }
            }
        }

        if (actions.isEmpty() == false) {
            automata.add(patterns(actions));
        }
        return new ClusterPrivilege(name, Automatons.unionAndMinimize(automata));
    }
}
