/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.index.mapper.FieldArrayContext;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.FieldSubsetReader;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Builds the automaton that applies to one index holding the given flattened fields. It filters both the Lucene field names the reader
 * exposes and the JSON paths kept in {@code _source} and {@code _ignored_source}: a flattened field stores all its keys under a few Lucene
 * field names ({@code labels}, {@code labels._keyed}, {@code labels.offsets} and their companions), which are exposed or hidden whole,
 * while the JSON walk descends into an object whenever the state after its name is live and so still drops hidden keys one by one. It is
 * built from the grant and except patterns, not from the merged automaton, since the merged automaton cannot tell "labels" granted as a
 * leaf apart from "labels.env" granted on its own.
 */
final class FlattenedAutomaton {

    private FlattenedAutomaton() {}

    /**
     * Returns the automaton for {@code definitions} applied to an index with the given flattened fields. Patterns follow leaf semantics: a
     * pattern that matches a flattened field's name or a path inside it covers everything below that path, for grants and excepts alike.
     * Within a group except wins, groups union, definitions intersect. Under partial access (the role sees some but not all paths below the
     * field) the field's Lucene storage is hidden whole, since it cannot be filtered by key; {@code _source} and {@code _ignored_source}
     * are still filtered per key. It is built on every call, so each search builds it when it wraps the index reader and field_caps once
     * per index (about 0.7 s for a thousand flattened fields in FlattenedAutomatonTests).
     */
    static Automaton forIndex(List<FieldPermissionsDefinition> definitions, Set<String> flattenedFields) {
        // Every automaton operation below runs once over all flattened fields at a time, never once per field. The fields are sorted so
        // the automaton, and the hash that field_caps compares across nodes, come out the same on every node.
        List<String> fields = flattenedFields.stream().sorted().toList();
        Automaton names = Automatons.unionAndMinimize(fields.stream().map(Automata::makeString).toList());
        Automaton covered = Automatons.unionAndMinimize(List.of(names, below(names)));
        Automaton paths = null;
        for (FieldPermissionsDefinition definition : definitions) {
            List<Automaton> groups = new ArrayList<>();
            for (FieldGrantExcludeGroup group : definition.getFieldGrantExcludeGroups()) {
                Automaton granted = expand(FieldPermissions.grantedFieldsAutomaton(group.getGrantedFields()), covered);
                Automaton denied = expand(FieldPermissions.deniedFieldsAutomaton(group.getExcludedFields()), covered);
                groups.add(Automatons.minusAndMinimize(granted, denied));
            }
            Automaton definitionPaths = Automatons.unionAndMinimize(groups);
            paths = paths == null ? definitionPaths : Automatons.intersectAndMinimize(paths, definitionPaths);
        }

        // A field is partially visible when some path below it is hidden. The hidden paths below all flattened fields form one minimized
        // automaton, so it has no dead states and reaching a live state through "field." means a hidden path lies below that field.
        CharacterRunAutomaton hiddenBelow = new CharacterRunAutomaton(Automatons.minusAndMinimize(below(names), paths));
        List<Automaton> partial = fields.stream()
            .filter(field -> FieldSubsetReader.step(hiddenBelow, field + ".", 0) != -1)
            .map(Automata::makeString)
            .toList();

        // Every field's name is added, which exposes the keyless root values under full access; its keyed storage is already in paths
        // since nothing below the field is hidden. Partial access removes the name again, hiding the root values, along with the keyed
        // storage and its companion fields (ex. .offsets, .counts).
        String keyed = FlattenedFieldMapper.KEYED_FIELD_SUFFIX;
        String offsets = FieldArrayContext.OFFSETS_FIELD_NAME_SUFFIX;
        String counts = MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;
        Automaton storage = Operations.optional(Automatons.patterns(keyed, keyed + ".*", offsets, offsets + counts, counts));
        return Automatons.minusAndMinimize(
            Automatons.unionAndMinimize(List.of(paths, names)),
            Operations.concatenate(List.of(Automatons.unionAndMinimize(partial), storage))
        );
    }

    /**
     * Adds, for every flattened field, everything below each path that {@code patterns} matches at or inside that field: a pattern that
     * matches {@code labels} also covers {@code labels.env}, and one that matches {@code labels.user} also covers {@code labels.user.name}.
     */
    private static Automaton expand(Automaton patterns, Automaton covered) {
        return Automatons.unionAndMinimize(List.of(patterns, below(Operations.intersection(patterns, covered))));
    }

    /** Accepts every path below each string {@code names} accepts, but not those strings themselves. */
    private static Automaton below(Automaton names) {
        return Operations.concatenate(List.of(names, Automata.makeChar('.'), Automata.makeAnyString()));
    }
}
