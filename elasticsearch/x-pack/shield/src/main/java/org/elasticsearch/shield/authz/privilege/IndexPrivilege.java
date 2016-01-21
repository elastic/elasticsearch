/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.privilege;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.common.Strings;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Predicate;

/**
 *
 */
public class IndexPrivilege extends AbstractAutomatonPrivilege<IndexPrivilege> {

    public static final IndexPrivilege NONE = new IndexPrivilege(Name.NONE, BasicAutomata.makeEmpty());
    public static final IndexPrivilege ALL = new IndexPrivilege(Name.ALL, "indices:*");
    public static final IndexPrivilege MANAGE = new IndexPrivilege("manage", "indices:monitor/*", "indices:admin/*");
    public static final IndexPrivilege CREATE_INDEX = new IndexPrivilege("create_index", CreateIndexAction.NAME);
    public static final IndexPrivilege MANAGE_ALIASES = new IndexPrivilege("manage_aliases", "indices:admin/aliases*");
    public static final IndexPrivilege MONITOR = new IndexPrivilege("monitor", "indices:monitor/*");
    public static final IndexPrivilege DATA_ACCESS = new IndexPrivilege("data_access", "indices:data/*");
    public static final IndexPrivilege CRUD = new IndexPrivilege("crud", "indices:data/write/*", "indices:data/read/*");
    public static final IndexPrivilege READ = new IndexPrivilege("read", "indices:data/read/*");
    public static final IndexPrivilege SEARCH = new IndexPrivilege("search", SearchAction.NAME + "*", MultiSearchAction.NAME + "*", SuggestAction.NAME + "*");
    public static final IndexPrivilege GET = new IndexPrivilege("get", GetAction.NAME + "*", MultiGetAction.NAME + "*");
    public static final IndexPrivilege SUGGEST = new IndexPrivilege("suggest", SuggestAction.NAME + "*");
    public static final IndexPrivilege INDEX = new IndexPrivilege("index", "indices:data/write/index*", "indices:data/write/update*");
    public static final IndexPrivilege DELETE = new IndexPrivilege("delete", "indices:data/write/delete*");
    public static final IndexPrivilege WRITE = new IndexPrivilege("write", "indices:data/write/*");

    private static final Set<IndexPrivilege> values = new CopyOnWriteArraySet<>();

    static {
        values.add(NONE);
        values.add(ALL);
        values.add(MANAGE);
        values.add(CREATE_INDEX);
        values.add(MANAGE_ALIASES);
        values.add(MONITOR);
        values.add(DATA_ACCESS);
        values.add(CRUD);
        values.add(READ);
        values.add(SEARCH);
        values.add(GET);
        values.add(SUGGEST);
        values.add(INDEX);
        values.add(DELETE);
        values.add(WRITE);
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

    private IndexPrivilege(Name name, String... patterns) {
        super(name, patterns);
    }

    private IndexPrivilege(Name name, Automaton automaton) {
        super(name, automaton);
    }

    public static void addCustom(String name, String... actionPatterns) {
        for (String pattern : actionPatterns) {
            if (!IndexPrivilege.ACTION_MATCHER.test(pattern)) {
                throw new IllegalArgumentException("cannot register custom index privilege [" + name + "]. index action must follow the 'indices:*' format");
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
