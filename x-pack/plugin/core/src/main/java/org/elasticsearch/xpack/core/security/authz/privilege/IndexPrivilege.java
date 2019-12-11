/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.ccr.action.ForgetFollowerAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.core.security.support.Automatons.patterns;
import static org.elasticsearch.xpack.core.security.support.Automatons.unionAndMinimize;

public final class IndexPrivilege extends Privilege {
    private static final Logger logger = LogManager.getLogger(IndexPrivilege.class);

    private static final Automaton ALL_AUTOMATON = patterns("indices:*", "internal:transport/proxy/indices:*");
    private static final Automaton READ_AUTOMATON = patterns("indices:data/read/*");
    private static final Automaton READ_CROSS_CLUSTER_AUTOMATON = patterns("internal:transport/proxy/indices:data/read/*",
            ClusterSearchShardsAction.NAME);
    private static final Automaton CREATE_AUTOMATON = patterns("indices:data/write/index*", "indices:data/write/bulk*",
            PutMappingAction.NAME);
    private static final Automaton CREATE_DOC_AUTOMATON = patterns("indices:data/write/index", "indices:data/write/index[*",
        "indices:data/write/index:op_type/create", "indices:data/write/bulk*", PutMappingAction.NAME);
    private static final Automaton INDEX_AUTOMATON =
            patterns("indices:data/write/index*", "indices:data/write/bulk*", "indices:data/write/update*", PutMappingAction.NAME);
    private static final Automaton DELETE_AUTOMATON = patterns("indices:data/write/delete*", "indices:data/write/bulk*");
    private static final Automaton WRITE_AUTOMATON = patterns("indices:data/write/*", PutMappingAction.NAME);
    private static final Automaton MONITOR_AUTOMATON = patterns("indices:monitor/*");
    private static final Automaton MANAGE_AUTOMATON =
            unionAndMinimize(Arrays.asList(MONITOR_AUTOMATON, patterns("indices:admin/*")));
    private static final Automaton CREATE_INDEX_AUTOMATON = patterns(CreateIndexAction.NAME);
    private static final Automaton DELETE_INDEX_AUTOMATON = patterns(DeleteIndexAction.NAME);
    private static final Automaton VIEW_METADATA_AUTOMATON = patterns(GetAliasesAction.NAME,
            GetIndexAction.NAME, GetFieldMappingsAction.NAME + "*", GetMappingsAction.NAME,
            ClusterSearchShardsAction.NAME, ValidateQueryAction.NAME + "*", GetSettingsAction.NAME, ExplainLifecycleAction.NAME);
    private static final Automaton MANAGE_FOLLOW_INDEX_AUTOMATON = patterns(PutFollowAction.NAME, UnfollowAction.NAME,
        CloseIndexAction.NAME + "*");
    private static final Automaton MANAGE_LEADER_INDEX_AUTOMATON = patterns(ForgetFollowerAction.NAME + "*");
    private static final Automaton MANAGE_ILM_AUTOMATON = patterns("indices:admin/ilm/*");

    public static final IndexPrivilege NONE =                new IndexPrivilege("none",                Automatons.EMPTY);
    public static final IndexPrivilege ALL =                 new IndexPrivilege("all",                 ALL_AUTOMATON);
    public static final IndexPrivilege READ =                new IndexPrivilege("read",                READ_AUTOMATON);
    public static final IndexPrivilege READ_CROSS_CLUSTER =  new IndexPrivilege("read_cross_cluster",  READ_CROSS_CLUSTER_AUTOMATON);
    public static final IndexPrivilege CREATE =              new IndexPrivilege("create",              CREATE_AUTOMATON);
    public static final IndexPrivilege INDEX =               new IndexPrivilege("index",               INDEX_AUTOMATON);
    public static final IndexPrivilege DELETE =              new IndexPrivilege("delete",              DELETE_AUTOMATON);
    public static final IndexPrivilege WRITE =               new IndexPrivilege("write",               WRITE_AUTOMATON);
    public static final IndexPrivilege CREATE_DOC =          new IndexPrivilege("create_doc",          CREATE_DOC_AUTOMATON);
    public static final IndexPrivilege MONITOR =             new IndexPrivilege("monitor",             MONITOR_AUTOMATON);
    public static final IndexPrivilege MANAGE =              new IndexPrivilege("manage",              MANAGE_AUTOMATON);
    public static final IndexPrivilege DELETE_INDEX =        new IndexPrivilege("delete_index",        DELETE_INDEX_AUTOMATON);
    public static final IndexPrivilege CREATE_INDEX =        new IndexPrivilege("create_index",        CREATE_INDEX_AUTOMATON);
    public static final IndexPrivilege VIEW_METADATA =       new IndexPrivilege("view_index_metadata", VIEW_METADATA_AUTOMATON);
    public static final IndexPrivilege MANAGE_FOLLOW_INDEX = new IndexPrivilege("manage_follow_index", MANAGE_FOLLOW_INDEX_AUTOMATON);
    public static final IndexPrivilege MANAGE_LEADER_INDEX = new IndexPrivilege("manage_leader_index", MANAGE_LEADER_INDEX_AUTOMATON);
    public static final IndexPrivilege MANAGE_ILM =          new IndexPrivilege("manage_ilm",          MANAGE_ILM_AUTOMATON);

    private static final Map<String, IndexPrivilege> VALUES = Map.ofEntries(
            entry("none", NONE),
            entry("all", ALL),
            entry("manage", MANAGE),
            entry("create_index", CREATE_INDEX),
            entry("monitor", MONITOR),
            entry("read", READ),
            entry("index", INDEX),
            entry("delete", DELETE),
            entry("write", WRITE),
            entry("create", CREATE),
            entry("create_doc", CREATE_DOC),
            entry("delete_index", DELETE_INDEX),
            entry("view_index_metadata", VIEW_METADATA),
            entry("read_cross_cluster", READ_CROSS_CLUSTER),
            entry("manage_follow_index", MANAGE_FOLLOW_INDEX),
            entry("manage_leader_index", MANAGE_LEADER_INDEX),
            entry("manage_ilm", MANAGE_ILM));

    public static final Predicate<String> ACTION_MATCHER = ALL.predicate();
    public static final Predicate<String> CREATE_INDEX_MATCHER = CREATE_INDEX.predicate();

    private static final ConcurrentHashMap<Set<String>, IndexPrivilege> CACHE = new ConcurrentHashMap<>();

    private IndexPrivilege(String name, Automaton automaton) {
        super(Collections.singleton(name), automaton);
    }

    private IndexPrivilege(Set<String> name, Automaton automaton) {
        super(name, automaton);
    }

    public static IndexPrivilege get(Set<String> name) {
        return CACHE.computeIfAbsent(name, (theName) -> {
            if (theName.isEmpty()) {
                return NONE;
            } else {
                return resolve(theName);
            }
        });
    }

    private static IndexPrivilege resolve(Set<String> name) {
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
                IndexPrivilege indexPrivilege = VALUES.get(part);
                if (indexPrivilege != null && size == 1) {
                    return indexPrivilege;
                } else if (indexPrivilege != null) {
                    automata.add(indexPrivilege.automaton);
                } else {
                    String errorMessage = "unknown index privilege [" + part + "]. a privilege must be either " +
                        "one of the predefined fixed indices privileges [" +
                        Strings.collectionToCommaDelimitedString(VALUES.entrySet()) + "] or a pattern over one of the available index" +
                        " actions";
                    logger.debug(errorMessage);
                    throw new IllegalArgumentException(errorMessage);
                }
            }
        }

        if (actions.isEmpty() == false) {
            automata.add(patterns(actions));
        }
        return new IndexPrivilege(name, unionAndMinimize(automata));
    }

    static Map<String, IndexPrivilege> values() {
        return VALUES;
    }

    public static Set<String> names() {
        return Collections.unmodifiableSet(VALUES.keySet());
    }

}
