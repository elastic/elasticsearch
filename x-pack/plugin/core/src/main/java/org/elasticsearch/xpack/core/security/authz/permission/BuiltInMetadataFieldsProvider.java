/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Set;

public class BuiltInMetadataFieldsProvider {

    private final Set<String> allMetadataFields;
    private final Automaton allMetadataFieldsAutomaton;

    public BuiltInMetadataFieldsProvider(Set<String> allMetadataFields) {
        this.allMetadataFields = allMetadataFields;
        this.allMetadataFieldsAutomaton = Automatons.patterns(allMetadataFields);
    }

    public boolean isMetadataField(String field) {
        return allMetadataFields.contains(field);
    }

    public Automaton getAutomaton() {
        return allMetadataFieldsAutomaton;
    }

}
