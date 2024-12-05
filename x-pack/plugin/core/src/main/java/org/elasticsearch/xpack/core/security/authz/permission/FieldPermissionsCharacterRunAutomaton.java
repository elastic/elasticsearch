/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

public class FieldPermissionsCharacterRunAutomaton extends CharacterRunAutomaton {

    private final CharacterRunAutomaton permittedFieldsAutomaton;
    private final BuiltInMetadataFieldsProvider builtInMetadataFields;

    public FieldPermissionsCharacterRunAutomaton(
        CharacterRunAutomaton permittedFieldsAutomaton,
        BuiltInMetadataFieldsProvider builtInMetadataFields
    ) {
        super(Automata.makeEmpty());
        this.permittedFieldsAutomaton = permittedFieldsAutomaton;
        this.builtInMetadataFields = builtInMetadataFields;
    }

    @Override
    public boolean run(String s) {
        return permittedFieldsAutomaton.run(s) || builtInMetadataFields.getRunAutomaton().run(s);
    }

    public boolean run(char[] s, int offset, int length) {
        return permittedFieldsAutomaton.run(s, offset, length) || builtInMetadataFields.getRunAutomaton().run(s, offset, length);
    }

    @Override
    public long ramBytesUsed() {
        return permittedFieldsAutomaton.ramBytesUsed() + builtInMetadataFields.getRunAutomaton().ramBytesUsed();
    }
}
