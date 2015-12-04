/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;
import dk.brics.automaton.BasicOperations;

import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.suggest.SuggestAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Predicate;

import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.shield.support.Automatons.patterns;

/**
 *
 */
public abstract class Privilege<P extends Privilege<P>> {

    static final String SUB_ACTION_SUFFIX_PATTERN = "*";

    public static final System SYSTEM = new System();
    public static final General HEALTH_AND_STATS = new General("health_and_stats",
            "cluster:monitor/health*",
            "cluster:monitor/stats*",
            "indices:monitor/stats*",
            "cluster:monitor/nodes/stats*");

    protected final Name name;

    private Privilege(Name name) {
        this.name = name;
    }

    public Name name() {
        return name;
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

    public abstract Predicate<String> predicate();

    public abstract boolean implies(P other);

    @SuppressWarnings("unchecked")
    public boolean isAlias(P other) {
        return this.implies(other) && other.implies((P) this);
    }

    public static class System extends Privilege<System> {

        private static final Predicate<String> INTERNAL_PREDICATE = new AutomatonPredicate(patterns("internal:*"));

        protected static final Predicate<String> PREDICATE = new AutomatonPredicate(patterns(
                "internal:*",
                "indices:monitor/*", // added for marvel
                "cluster:monitor/*",  // added for marvel
                "cluster:admin/reroute", // added for DiskThresholdDecider.DiskListener
                "indices:admin/mapping/put" // ES 2.0 MappingUpdatedAction - updateMappingOnMasterSynchronously
        ));

        private System() {
            super(new Name("internal"));
        }

        @Override
        public Predicate<String> predicate() {
            return PREDICATE;
        }

        @Override
        public boolean implies(System other) {
            return true;
        }
    }

    public static class General extends AutomatonPrivilege<General> {

        public static final General NONE = new General(Name.NONE, BasicAutomata.makeEmpty());

        public General(String name, String... patterns) {
            super(name, patterns);
        }

        public General(Name name, String... patterns) {
            super(name, patterns);
        }

        public General(Name name, Automaton automaton) {
            super(name, automaton);
        }

        @Override
        protected General create(Name name, Automaton automaton) {
            return new General(name, automaton);
        }

        @Override
        protected General none() {
            return NONE;
        }
    }


    public static class Index extends AutomatonPrivilege<Index> {

        public static final Index NONE =            new Index(Name.NONE,        BasicAutomata.makeEmpty());
        public static final Index ALL =             new Index(Name.ALL,         "indices:*");
        public static final Index MANAGE =          new Index("manage",         "indices:monitor/*", "indices:admin/*");
        public static final Index CREATE_INDEX =    new Index("create_index",   CreateIndexAction.NAME);
        public static final Index MANAGE_ALIASES =  new Index("manage_aliases", "indices:admin/aliases*");
        public static final Index MONITOR =         new Index("monitor",        "indices:monitor/*");
        public static final Index DATA_ACCESS =     new Index("data_access",    "indices:data/*");
        public static final Index CRUD =            new Index("crud",           "indices:data/write/*", "indices:data/read/*");
        public static final Index READ =            new Index("read",           "indices:data/read/*");
        public static final Index SEARCH =          new Index("search",         SearchAction.NAME + "*", MultiSearchAction.NAME + "*", SuggestAction.NAME + "*");
        public static final Index GET =             new Index("get",            GetAction.NAME + "*", MultiGetAction.NAME + "*");
        public static final Index SUGGEST =         new Index("suggest",        SuggestAction.NAME + "*");
        public static final Index INDEX =           new Index("index",          "indices:data/write/index*", "indices:data/write/update*");
        public static final Index DELETE =          new Index("delete",         "indices:data/write/delete*");
        public static final Index WRITE =           new Index("write",          "indices:data/write/*");

        private static final Set<Index> values = new CopyOnWriteArraySet<>();
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

        static Set<Index> values() {
            return values;
        }

        private static final ConcurrentHashMap<Name, Index> cache = new ConcurrentHashMap<>();

        private Index(String name, String... patterns) {
            super(name, patterns);
        }

        private Index(Name name, String... patterns) {
            super(name, patterns);
        }

        private Index(Name name, Automaton automaton) {
            super(name, automaton);
        }

        public static void addCustom(String name, String... actionPatterns) {
            for (String pattern : actionPatterns) {
                if (!Index.ACTION_MATCHER.test(pattern)) {
                    throw new IllegalArgumentException("cannot register custom index privilege [" + name + "]. index action must follow the 'indices:*' format");
                }
            }
            Index custom = new Index(name, actionPatterns);
            if (values.contains(custom)) {
                throw new IllegalArgumentException("cannot register custom index privilege [" + name + "] as it already exists.");
            }
            values.add(custom);
        }

        @Override
        protected Index create(Name name, Automaton automaton) {
            if (name == Name.NONE) {
                return NONE;
            }
            return new Index(name, automaton);
        }

        @Override
        protected Index none() {
            return NONE;
        }

        public static Index action(String action) {
            return new Index(action, actionToPattern(action));
        }

        public static Index get(Name name) {
            return cache.computeIfAbsent(name, (theName) -> {
                Index index = NONE;
                for (String part : theName.parts) {
                    index = index == NONE ? resolve(part) : index.plus(resolve(part));
                }
                return index;
            });
        }

        public static Index union(Index... indices) {
            Index result = NONE;
            for (Index index : indices) {
                result = result.plus(index);
            }
            return result;
        }

        private static Index resolve(String name) {
            name = name.toLowerCase(Locale.ROOT);
            if (ACTION_MATCHER.test(name)) {
                return action(name);
            }
            for (Index index : values) {
                if (name.toLowerCase(Locale.ROOT).equals(index.name.toString())) {
                    return index;
                }
            }
            throw new IllegalArgumentException("unknown index privilege [" + name + "]. a privilege must be either " +
                    "one of the predefined fixed indices privileges [" + Strings.collectionToCommaDelimitedString(values) +
                    "] or a pattern over one of the available index actions");
        }

    }

    public static class Cluster extends AutomatonPrivilege<Cluster> {

        public static final Cluster NONE    = new Cluster(Name.NONE,                BasicAutomata.makeEmpty());
        public static final Cluster ALL     = new Cluster(Name.ALL,                 "cluster:*", "indices:admin/template/*");
        public static final Cluster MONITOR = new Cluster("monitor",                "cluster:monitor/*");
        public static final Cluster MANAGE_SHIELD = new Cluster("manage_shield",    "cluster:admin/shield/*");

        final static Predicate<String> ACTION_MATCHER = Privilege.Cluster.ALL.predicate();

        private static final Set<Cluster> values = new CopyOnWriteArraySet<>();
        static {
            values.add(NONE);
            values.add(ALL);
            values.add(MONITOR);
            values.add(MANAGE_SHIELD);
        }

        static Set<Cluster> values() {
            return values;
        }

        private static final ConcurrentHashMap<Name, Cluster> cache = new ConcurrentHashMap<>();


        private Cluster(String name, String... patterns) {
            super(name, patterns);
        }

        private Cluster(Name name, String... patterns) {
            super(name, patterns);
        }

        private Cluster(Name name, Automaton automaton) {
            super(name, automaton);
        }

        public static void addCustom(String name, String... actionPatterns) {
            for (String pattern : actionPatterns) {
                if (!Cluster.ACTION_MATCHER.test(pattern)) {
                    throw new IllegalArgumentException("cannot register custom cluster privilege [" + name + "]. cluster aciton must follow the 'cluster:*' format");
                }
            }
            Cluster custom = new Cluster(name, actionPatterns);
            if (values.contains(custom)) {
                throw new IllegalArgumentException("cannot register custom cluster privilege [" + name + "] as it already exists.");
            }
            values.add(custom);
        }

        @Override
        protected Cluster create(Name name, Automaton automaton) {
            return new Cluster(name, automaton);
        }

        @Override
        protected Cluster none() {
            return NONE;
        }

        public static Cluster action(String action) {
            String pattern = actionToPattern(action);
            return new Cluster(action, pattern);
        }

        public static Cluster get(Name name) {
            return cache.computeIfAbsent(name, (theName) -> {
                Cluster cluster = NONE;
                for (String part : theName.parts) {
                    cluster = cluster == NONE ? resolve(part) : cluster.plus(resolve(part));
                }
                return cluster;
            });
        }

        private static Cluster resolve(String name) {
            name = name.toLowerCase(Locale.ROOT);
            if (ACTION_MATCHER.test(name)) {
                return action(name);
            }
            for (Cluster cluster : values) {
                if (name.equals(cluster.name.toString())) {
                    return cluster;
                }
            }
            throw new IllegalArgumentException("unknown cluster privilege [" + name + "]. a privilege must be either " +
                    "one of the predefined fixed cluster privileges [" + Strings.collectionToCommaDelimitedString(values) +
                    "] or a pattern over one of the available cluster actions");
        }
    }

    static String actionToPattern(String text) {
        return text + SUB_ACTION_SUFFIX_PATTERN;
    }

    @SuppressWarnings("unchecked")
    private static abstract class AutomatonPrivilege<P extends AutomatonPrivilege<P>> extends Privilege<P> {

        protected final Automaton automaton;

        private AutomatonPrivilege(String name, String... patterns) {
            super(new Name(name));
            this.automaton = patterns(patterns);
        }

        private AutomatonPrivilege(Name name, String... patterns) {
            super(name);
            this.automaton = patterns(patterns);
        }

        private AutomatonPrivilege(Name name, Automaton automaton) {
            super(name);
            this.automaton = automaton;
        }

        @Override
        public Predicate<String> predicate() {
            return new AutomatonPredicate(automaton);
        }

        protected P plus(P other) {
            if (other.implies((P) this)) {
                return other;
            }
            if (this.implies(other)) {
                return (P) this;
            }
            return create(name.add(other.name), Automatons.unionAndDeterminize(automaton, other.automaton));
        }

        protected P minus(P other) {
            if (other.implies((P) this)) {
                return none();
            }
            if (other == none() || !this.implies(other)) {
                return (P) this;
            }
            return create(name.remove(other.name), Automatons.minusAndDeterminize(automaton, other.automaton));
        }

        @Override
        public boolean implies(P other) {
            return BasicOperations.subsetOf(other.automaton, automaton);
        }

        @Override
        public String toString() {
            return name.toString();
        }

        protected abstract P create(Name name, Automaton automaton);

        protected abstract P none();


    }

    public static class Name {

        public static final Name NONE = new Name("none");
        public static final Name ALL = new Name("all");

        private final Set<String> parts;

        public Name(String name) {
            assert name != null && !name.contains(",");
            parts = singleton(name);
        }

        public Name(Set<String> parts) {
            assert !parts.isEmpty();
            this.parts = unmodifiableSet(new HashSet<>(parts));
        }

        public Name(String... parts) {
            this(unmodifiableSet(newHashSet(parts)));
        }

        @Override
        public String toString() {
            return Strings.collectionToCommaDelimitedString(parts);
        }

        public Name add(Name other) {
            return new Name(Sets.union(parts, other.parts));
        }

        public Name remove(Name other) {
            Set<String> parts = Sets.difference(this.parts, other.parts);
            return parts.isEmpty() ? NONE : new Name(parts);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Name name = (Name) o;

            return parts.equals(name.parts);
        }

        @Override
        public int hashCode() {
            return parts.hashCode();
        }
    }
}
