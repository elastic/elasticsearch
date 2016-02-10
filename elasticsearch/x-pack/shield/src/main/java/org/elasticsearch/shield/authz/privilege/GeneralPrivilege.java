/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.privilege;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.BasicAutomata;

/**
 *
 */
public class GeneralPrivilege extends AbstractAutomatonPrivilege<GeneralPrivilege> {

    public static final GeneralPrivilege NONE = new GeneralPrivilege(Name.NONE, BasicAutomata.makeEmpty());
    public static final GeneralPrivilege ALL = new GeneralPrivilege(Name.ALL, "*");

    public GeneralPrivilege(String name, String... patterns) {
        super(name, patterns);
    }

    public GeneralPrivilege(Name name, String... patterns) {
        super(name, patterns);
    }

    public GeneralPrivilege(Name name, Automaton automaton) {
        super(name, automaton);
    }

    @Override
    protected GeneralPrivilege create(Name name, Automaton automaton) {
        return new GeneralPrivilege(name, automaton);
    }

    @Override
    protected GeneralPrivilege none() {
        return NONE;
    }
}
