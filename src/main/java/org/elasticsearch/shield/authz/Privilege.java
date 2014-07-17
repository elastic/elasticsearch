/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import com.google.common.base.Predicate;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.BasicAutomata;
import org.apache.lucene.util.automaton.MinimizationOperations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.shield.support.AutomatonPredicate;

/**
 *
 */
public interface Privilege {

    Privilege SYSTEM = new AutomatonPrivilege("internal:.*");

    Predicate<String> predicate();

    Privilege plus(Privilege other);

    Privilege minus(Privilege other);

    boolean implies(Privilege other);

    public static enum Index implements Privilege {

        NONE(AutomatonPrivilege.NONE),
        ALL("indices:.*"),
        MANAGE("indices:monitor/.*", "indices:admin/.*"),
        MONITOR("indices:monitor/.*"),

        FULL_DATA_ACCESS("indices:data/.*"),

        CRUD("indices:data/write/.*", "indices:data/read/.*"),
        READ("indices:data/read/.*"),
        CREATE("indices:data/write/create"),    //todo unsupported yet
        INDEX("indices:data/write/index.*", "indices:data/write/update"),
        DELETE("indices:data/write/delete.*"),
        WRITE("indices:data/write/.*"),

        BENCHMARK("indices:data/benchmark");

        private AutomatonPrivilege privilege;

        private Index(String... patterns) {
            this(new AutomatonPrivilege(patterns));
        }

        private Index(AutomatonPrivilege privilege) {
            this.privilege = privilege;
        }

        @Override
        public Predicate<String> predicate() {
            return privilege.predicate();
        }

        @Override
        public Privilege plus(Privilege other) {
            return privilege.plus(other);
        }

        @Override
        public Privilege minus(Privilege other) {
            return privilege.minus(other);
        }

        @Override
        public boolean implies(Privilege other) {
            return privilege.implies(other);
        }

        public static Index resolve(String... names) {
            Index result = null;
            for (int i = 0; i < names.length; i++) {
                if (result == null) {
                    result = Index.valueOf(names[i]);
                } else {
                    result.plus(Index.valueOf(names[i]));
                }
            }
            return result;
        }
    }

    public static enum Cluster implements Privilege {

        NONE(AutomatonPrivilege.NONE),
        ALL("cluster:.*"),
        MANAGE("cluster:.*"),
        MONITOR("cluster:monitor/.*");

        private AutomatonPrivilege privilege;

        private Cluster(String... patterns) {
            this(new AutomatonPrivilege(patterns));
        }

        private Cluster(AutomatonPrivilege privilege) {
            this.privilege = privilege;
        }

        @Override
        public Predicate<String> predicate() {
            return privilege.predicate();
        }

        @Override
        public Privilege plus(Privilege other) {
            return privilege.plus(other);
        }

        @Override
        public Privilege minus(Privilege other) {
            return privilege.minus(other);
        }

        @Override
        public boolean implies(Privilege other) {
            return privilege.implies(other);
        }

        public static Cluster resolve(String... names) {
            Cluster result = null;
            for (int i = 0; i < names.length; i++) {
                if (result == null) {
                    result = Cluster.valueOf(names[i]);
                } else {
                    result.plus(Cluster.valueOf(names[i]));
                }
            }
            return result;
        }
    }

    static class AutomatonPrivilege implements Privilege {

        private static final AutomatonPrivilege NONE = new AutomatonPrivilege(BasicAutomata.makeEmpty());

        private final Automaton automaton;

        private AutomatonPrivilege(String... patterns) {
            this.automaton = compileAutomaton(patterns);
        }

        private AutomatonPrivilege(Automaton automaton) {
            this.automaton = automaton;
        }

        @Override
        public Predicate<String> predicate() {
            return new AutomatonPredicate(automaton);
        }

        private static Automaton compileAutomaton(String... patterns) {
            Automaton a = null;
            for (int i = 0; i < patterns.length; i++) {
                if (a == null) {
                    a = new RegExp(patterns[i], RegExp.ALL).toAutomaton();
                } else {
                    a = a.union(new RegExp(patterns[i], RegExp.ALL).toAutomaton());
                }
            }
            MinimizationOperations.minimize(a);
            return a;
        }

        @Override
        public Privilege plus(Privilege other) {
            return new AutomatonPrivilege(automaton.union(((AutomatonPrivilege) other).automaton));
        }

        @Override
        public Privilege minus(Privilege other) {
            if (!implies(other)) {
                return this;
            }
            return new AutomatonPrivilege(automaton.minus(((AutomatonPrivilege) other).automaton));
        }

        @Override
        public boolean implies(Privilege other) {
            return ((AutomatonPrivilege) other).automaton.subsetOf(automaton);
        }
    }

}
