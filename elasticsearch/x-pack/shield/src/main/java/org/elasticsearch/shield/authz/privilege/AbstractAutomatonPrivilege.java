/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.privilege;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicOperations;
import org.elasticsearch.shield.support.AutomatonPredicate;
import org.elasticsearch.shield.support.Automatons;

import java.util.function.Predicate;

import static org.elasticsearch.shield.support.Automatons.patterns;

/**
 *
 */
@SuppressWarnings("unchecked")
abstract class AbstractAutomatonPrivilege<P extends AbstractAutomatonPrivilege<P>> extends Privilege<P> {

    protected final Automaton automaton;

    AbstractAutomatonPrivilege(String name, String... patterns) {
        super(new Name(name));
        this.automaton = patterns(patterns);
    }

    AbstractAutomatonPrivilege(Name name, String... patterns) {
        super(name);
        this.automaton = patterns(patterns);
    }

    AbstractAutomatonPrivilege(Name name, Automaton automaton) {
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
