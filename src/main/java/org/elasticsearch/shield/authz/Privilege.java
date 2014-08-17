/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.cache.CacheLoader;
import org.elasticsearch.common.cache.LoadingCache;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.util.concurrent.UncheckedExecutionException;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;

import java.util.Locale;
import java.util.Set;

/**
 *
 */
public abstract class Privilege<P extends Privilege<P>> {

    static final String SUB_ACTION_SUFFIX_PATTERN = ".*";

    public static final Internal INTERNAL = new Internal();

    protected final Name name;

    private Privilege(Name name) {
        this.name = name;
    }

    public Name name() {
        return name;
    }

    public abstract Predicate<String> predicate();

    public abstract boolean implies(P other);

    @SuppressWarnings("unchecked")
    public boolean isAlias(P other) {
        return this.implies(other) && other.implies((P) this);
    }

    public static class Internal extends Privilege<Internal> {

        protected static final Predicate<String> PREDICATE = new AutomatonPredicate(Automatons.patterns("internal:.*"));

        private Internal() {
            super(new Name("internal"));
        }

        @Override
        public Predicate<String> predicate() {
            return PREDICATE;
        }

        @Override
        public boolean implies(Internal other) {
            return true;
        }
    }

    public static class Index extends AutomatonPrivilege<Index> {

        public static final Index NONE =        new Index(Name.NONE,    BasicAutomata.makeEmpty());
        public static final Index ALL =         new Index(Name.ALL,     "indices:.*");
        public static final Index MANAGE =      new Index("manage",     "indices:monitor/.*", "indices:admin/.*");
        public static final Index MONITOR =     new Index("monitor",    "indices:monitor/.*");
        public static final Index DATA_ACCESS = new Index("data_access","indices:data/.*");
        public static final Index CRUD =        new Index("crud",       "indices:data/write/.*", "indices:data/read/.*");
        public static final Index READ =        new Index("read",       "indices:data/read/.*");
        public static final Index SEARCH =      new Index("search",       SearchAction.NAME + ".*", GetAction.NAME + ".*");
        public static final Index GET =         new Index("get",       GetAction.NAME + ".*");
        public static final Index INDEX =       new Index("index",      "indices:data/write/index.*", "indices:data/write/update");
        public static final Index DELETE =      new Index("delete",     "indices:data/write/delete.*");
        public static final Index WRITE =       new Index("write",      "indices:data/write/.*");
        public static final Index BENCHMARK =   new Index("benchmark",  "indices:data/benchmark");

        private static final Index[] values = new Index[] {
            NONE, ALL, MANAGE, MONITOR, DATA_ACCESS, CRUD, READ, SEARCH, GET, INDEX, DELETE, WRITE, BENCHMARK
        };

        static Index[] values() {
            return values;
        }

        private static final LoadingCache<Name, Index> cache = CacheBuilder.newBuilder().build(
                new CacheLoader<Name, Index>() {
                    @Override
                    public Index load(Name name) throws Exception {
                        Index index = NONE;
                        for (String part : name.parts) {
                            index = index == NONE ? resolve(part) : index.plus(resolve(part));
                        }
                        return index;
                    }
                });

        private Index(String name, String... patterns) {
            super(name, patterns);
        }

        private Index(Name name, String... patterns) {
            super(name, patterns);
        }

        private Index(Name name, Automaton automaton) {
            super(name, automaton);
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
            try {
                return cache.getUnchecked(name);
            } catch (UncheckedExecutionException e) {
                throw (ElasticsearchException) e.getCause();
            }
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
            if (name.startsWith("indices:")) {
                return action(name);
            }
            for (Index index : values) {
                if (name.toLowerCase(Locale.ROOT).equals(index.name.toString())) {
                    return index;
                }
            }
            throw new ElasticsearchIllegalArgumentException("Unknown index privilege [" + name + "]. A privilege must either " +
                    "on of the predefined fixed indices privileges [" + Strings.arrayToCommaDelimitedString(values) +
                    "] or a pattern over one of the available index actions (the pattern must begin with \"indices:\" prefix)");
        }

    }

    public static class Cluster extends AutomatonPrivilege<Cluster> {

        public static final Cluster NONE    = new Cluster(Name.NONE,    BasicAutomata.makeEmpty());
        public static final Cluster ALL     = new Cluster(Name.ALL,     "cluster:.*", "indices:admin/template/.*");
        public static final Cluster MONITOR = new Cluster("monitor",    "cluster:monitor/.*");

        private static final Cluster[] values = new Cluster[] { NONE, ALL, MONITOR };

        static Cluster[] values() {
            return values;
        };

        private static final LoadingCache<Name, Cluster> cache = CacheBuilder.newBuilder().build(
                new CacheLoader<Name, Cluster>() {
                    @Override
                    public Cluster load(Name name) throws Exception {
                        Cluster cluster = NONE;
                        for (String part : name.parts) {
                            cluster = cluster == NONE ? resolve(part) : cluster.plus(resolve(part));
                        }
                        return cluster;
                    }
                });

        private Cluster(String name, String... patterns) {
            super(name, patterns);
        }

        private Cluster(Name name, String... patterns) {
            super(name, patterns);
        }

        private Cluster(Name name, Automaton automaton) {
            super(name, automaton);
        }

        @Override
        protected Cluster create(Name name, Automaton automaton) {
            if (name == Name.NONE) {
                return NONE;
            }
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
            try {
                return cache.getUnchecked(name);
            } catch (UncheckedExecutionException e) {
                throw (ElasticsearchException) e.getCause();
            }
        }

        private static Cluster resolve(String name) {
            name = name.toLowerCase(Locale.ROOT);
            if (name.startsWith("cluster:")) {
                return action(name);
            }
            for (Cluster cluster : values) {
                if (name.equals(cluster.name.toString())) {
                    return cluster;
                }
            }
            throw new ElasticsearchIllegalArgumentException("Unknown cluster privilege [" + name + "]. A privilege must either " +
                    "on of the predefined fixed cluster privileges [" + Strings.arrayToCommaDelimitedString(values) +
                    "] or a pattern over one of the available cluster actions (the pattern must begin with \"cluster:\" prefix)");
        }
    }

    static String actionToPattern(String text) {
        return text.replace(":", "\\:") + SUB_ACTION_SUFFIX_PATTERN;
    }

    @SuppressWarnings("unchecked")
    private static abstract class AutomatonPrivilege<P extends AutomatonPrivilege<P>> extends Privilege<P> {

        protected final Automaton automaton;

        private AutomatonPrivilege(String name, String... patterns) {
            super(new Name(name));
            this.automaton = Automatons.patterns(patterns);
        }

        private AutomatonPrivilege(Name name, String... patterns) {
            super(name);
            this.automaton = Automatons.patterns(patterns);
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
            return create(name.add(other.name), automaton.union(other.automaton));
        }

        protected P minus(P other) {
            if (other.implies((P) this)) {
                return none();
            }
            if (other == none() || !this.implies(other)) {
                return (P) this;
            }
            return create(name.remove(other.name), automaton.minus(other.automaton));
        }

        @Override
        public boolean implies(P other) {
            return other.automaton.subsetOf(automaton);
        }

        public String toString() {
            return name.toString();
        }

        protected abstract P create(Name name, Automaton automaton);

        protected abstract P none();
    }

    public static class Name {

        public static final Name NONE = new Name("none");
        public static final Name ALL = new Name("all");

        private final ImmutableSet<String> parts;

        public Name(String name) {
            assert name != null && !name.contains(",");
            parts = ImmutableSet.of(name);
        }

        public Name(Set<String> parts) {
            assert !parts.isEmpty();
            this.parts = ImmutableSet.copyOf(parts);
        }

        public Name(String... parts) {
            this(ImmutableSet.copyOf(parts));
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