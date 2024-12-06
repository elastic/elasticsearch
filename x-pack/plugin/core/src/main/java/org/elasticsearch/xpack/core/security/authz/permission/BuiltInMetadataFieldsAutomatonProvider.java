/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class BuiltInMetadataFieldsAutomatonProvider {

    private static final Set<String> ADDITIONAL_METADATA_FIELDS = Set.of("_primary_term");

    private final Automaton allMetadataFieldsAutomaton;
    private final Set<String> allMetadataFields;

    public BuiltInMetadataFieldsAutomatonProvider(Set<String> allMetadataFields) {
        Set<String> all = new HashSet<>();
        all.addAll(allMetadataFields);
        all.addAll(ADDITIONAL_METADATA_FIELDS);
        this.allMetadataFieldsAutomaton = Automatons.patterns(all);
        this.allMetadataFields = Collections.unmodifiableSet(all);
    }

    public boolean isMetadataField(String field) {
        return allMetadataFields.contains(field);
    }

    public Automaton getAutomaton() {
        return allMetadataFieldsAutomaton;
    }

}
