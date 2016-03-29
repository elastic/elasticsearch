/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.privilege;

import dk.brics.automaton.Automaton;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.shield.support.Automatons;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Predicate;

import static org.elasticsearch.shield.support.Automatons.patterns;
import static org.elasticsearch.shield.support.Automatons.unionAndDeterminize;

/**
 *
 */
public class IndexPrivilege extends AbstractAutomatonPrivilege<IndexPrivilege> {

    private static final Automaton ALL_AUTOMATON = patterns("indices:*");
    private static final Automaton READ_AUTOMATON = patterns("indices:data/read/*");
    private static final Automaton CREATE_AUTOMATON = patterns("indices:data/write/index*", PutMappingAction.NAME);
    private static final Automaton INDEX_AUTOMATON =
            patterns("indices:data/write/index*", "indices:data/write/update*", PutMappingAction.NAME);
    private static final Automaton DELETE_AUTOMATON = patterns("indices:data/write/delete*");
    private static final Automaton WRITE_AUTOMATON = patterns("indices:data/write/*", PutMappingAction.NAME);
    private static final Automaton MONITOR_AUTOMATON = patterns("indices:monitor/*");
    private static final Automaton MANAGE_AUTOMATON = unionAndDeterminize(MONITOR_AUTOMATON, patterns("indices:admin/*"));
    private static final Automaton CREATE_INDEX_AUTOMATON = patterns(CreateIndexAction.NAME);
    private static final Automaton DELETE_INDEX_AUTOMATON = patterns(DeleteIndexAction.NAME);
    private static final Automaton VIEW_METADATA_AUTOMATON = patterns(GetAliasesAction.NAME, AliasesExistAction.NAME,
            GetIndexAction.NAME, IndicesExistsAction.NAME, GetFieldMappingsAction.NAME + "*", GetMappingsAction.NAME,
            ClusterSearchShardsAction.NAME, TypesExistsAction.NAME, ValidateQueryAction.NAME + "*", GetSettingsAction.NAME);

    public static final IndexPrivilege NONE =                new IndexPrivilege(Name.NONE,             Automatons.EMPTY);
    public static final IndexPrivilege ALL =                 new IndexPrivilege(Name.ALL,              ALL_AUTOMATON);
    public static final IndexPrivilege READ =                new IndexPrivilege("read",                READ_AUTOMATON);
    public static final IndexPrivilege CREATE =              new IndexPrivilege("create",              CREATE_AUTOMATON);
    public static final IndexPrivilege INDEX =               new IndexPrivilege("index",               INDEX_AUTOMATON);
    public static final IndexPrivilege DELETE =              new IndexPrivilege("delete",              DELETE_AUTOMATON);
    public static final IndexPrivilege WRITE =               new IndexPrivilege("write",               WRITE_AUTOMATON);
    public static final IndexPrivilege MONITOR =             new IndexPrivilege("monitor",             MONITOR_AUTOMATON);
    public static final IndexPrivilege MANAGE =              new IndexPrivilege("manage",              MANAGE_AUTOMATON);
    public static final IndexPrivilege DELETE_INDEX =        new IndexPrivilege("delete_index",        DELETE_INDEX_AUTOMATON);
    public static final IndexPrivilege CREATE_INDEX =        new IndexPrivilege("create_index",        CREATE_INDEX_AUTOMATON);
    public static final IndexPrivilege VIEW_METADATA =       new IndexPrivilege("view_index_metadata", VIEW_METADATA_AUTOMATON);

    private static final Set<IndexPrivilege> values = new CopyOnWriteArraySet<>();

    static {
        values.add(NONE);
        values.add(ALL);
        values.add(MANAGE);
        values.add(CREATE_INDEX);
        values.add(MONITOR);
        values.add(READ);
        values.add(INDEX);
        values.add(DELETE);
        values.add(WRITE);
        values.add(CREATE);
        values.add(DELETE_INDEX);
        values.add(VIEW_METADATA);
    }

    public static final Predicate<String> ACTION_MATCHER = ALL.predicate();
    public static final Predicate<String> CREATE_INDEX_MATCHER = CREATE_INDEX.predicate();

    static Set<IndexPrivilege> values() {
        return values;
    }

    private static final ConcurrentHashMap<Name, IndexPrivilege> cache = new ConcurrentHashMap<>();

    private IndexPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    private IndexPrivilege(String name, Automaton automaton) {
        super(new Name(name), automaton);
    }

    private IndexPrivilege(Name name, Automaton automaton) {
        super(name, automaton);
    }

    public static void addCustom(String name, String... actionPatterns) {
        for (String pattern : actionPatterns) {
            if (!IndexPrivilege.ACTION_MATCHER.test(pattern)) {
                throw new IllegalArgumentException("cannot register custom index privilege [" + name + "]." +
                        " index action must follow the 'indices:*' format");
            }
        }
        IndexPrivilege custom = new IndexPrivilege(name, actionPatterns);
        if (values.contains(custom)) {
            throw new IllegalArgumentException("cannot register custom index privilege [" + name + "] as it already exists.");
        }
        values.add(custom);
    }

    @Override
    protected IndexPrivilege create(Name name, Automaton automaton) {
        if (name == Name.NONE) {
            return NONE;
        }
        return new IndexPrivilege(name, automaton);
    }

    @Override
    protected IndexPrivilege none() {
        return NONE;
    }

    public static IndexPrivilege action(String action) {
        return new IndexPrivilege(action, actionToPattern(action));
    }

    public static IndexPrivilege get(Name name) {
        return cache.computeIfAbsent(name, (theName) -> {
            IndexPrivilege index = NONE;
            for (String part : theName.parts) {
                index = index == NONE ? resolve(part) : index.plus(resolve(part));
            }
            return index;
        });
    }

    public static IndexPrivilege union(IndexPrivilege... indices) {
        IndexPrivilege result = NONE;
        for (IndexPrivilege index : indices) {
            result = result.plus(index);
        }
        return result;
    }

    private static IndexPrivilege resolve(String name) {
        name = name.toLowerCase(Locale.ROOT);
        if (ACTION_MATCHER.test(name)) {
            return action(name);
        }
        for (IndexPrivilege index : values) {
            if (name.toLowerCase(Locale.ROOT).equals(index.name.toString())) {
                return index;
            }
        }
        throw new IllegalArgumentException("unknown index privilege [" + name + "]. a privilege must be either " +
                "one of the predefined fixed indices privileges [" + Strings.collectionToCommaDelimitedString(values) +
                "] or a pattern over one of the available index actions");
    }

}
