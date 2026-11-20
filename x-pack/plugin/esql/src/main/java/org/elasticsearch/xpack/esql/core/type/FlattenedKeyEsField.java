/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import java.util.Map;

/**
 * The key of a {@code flattened} field that no index maps as a field of its own, e.g. {@code attributes.host} under a
 * {@code flattened} {@code attributes}. It is a {@code keyword}.
 *
 * <h2>The rules for keys</h2>
 * Keys work like unmapped fields that are loaded ({@code SET unmapped_fields="load"}): the name belongs to the index that holds it, and
 * the column flows up from there.
 * <ol>
 *     <li><b>A key is a field of the index that holds it.</b> A name that the query uses, that no index maps as a leaf, and that is under
 *     a {@code flattened} field of an index is a {@code keyword} field of that index, wherever the index comes from: {@code FROM}, a
 *     lookup index or an enrich policy. Each index reads it the way it reads its other fields, so in a union of indices the keys of
 *     one fill the column beside the mapped or loaded values of another.</li>
 *     <li><b>A key is a column like any other.</b> A command replaces exactly the names it brings in. {@code LOOKUP JOIN} and
 *     {@code ENRICH} bring in a key of their {@code flattened} fields only where no column of that name exists:
 *     <ul>
 *         <li>mapped fields, computed columns, and keys that an earlier command referenced exist, so the names mean what they meant
 *         before the command; this is also what keeps queries that are valid without keys unchanged;</li>
 *         <li>a key that no earlier command referenced does not exist yet, so replacing a {@code flattened} field replaces its keys as
 *         well: after {@code FROM a | LOOKUP JOIN b ON id}, an {@code attributes.host} that nothing named before the join reads
 *         {@code b}'s {@code attributes}.</li>
 *     </ul></li>
 *     <li><b>Keys are not tracked through their parent.</b> {@code RENAME} renames the column and not its keys, {@code EVAL} replaces
 *     the column and not its keys, and a {@code KEEP} that does not list a key drops it, as for any column. A key that no index holds
 *     under that name is an unmapped field: an error, {@code null} or loaded from {@code _source}, as {@code unmapped_fields} says; a
 *     key an index holds wins over {@code nullify} and {@code load} for that index.</li>
 *     <li><b>Patterns never match keys, and {@code DROP} reaches no key.</b> Removing the key from inside the value is tracked in
 *     elastic/elasticsearch#166913.</li>
 * </ol>
 * Any other type that exposes names inside a value, such as {@code nested} once its sub-fields can be named, has to follow the same
 * rules: mark what it brings as a name inside a value the way this class does, and use the same checks in the analyzer. Do not add
 * another set of rules for it.
 *
 * <h2>Why a class of its own</h2>
 * The analyzer needs to tell a key from a mapped field or a computed column of the same name to apply rule 2. It travels as a plain
 * {@link KeywordEsField}, since only the coordinator needs to know: a data node reads a key as it reads any name under a
 * {@code flattened} field.
 */
public class FlattenedKeyEsField extends KeywordEsField {

    public FlattenedKeyEsField(KeywordEsField key) {
        super(
            key.getName(),
            key.getDataType(),
            key.getProperties(),
            key.isAggregatable(),
            key.getPrecision(),
            key.getNormalized(),
            key.isAlias(),
            key.getTimeSeriesFieldType(),
            key.isStaticallySingleValued()
        );
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return new FlattenedKeyEsField((KeywordEsField) super.withProperties(newProperties));
    }

    /**
     * Whether {@code field} is the key of a {@code flattened} field rather than a mapped field.
     */
    public static boolean isKey(EsField field) {
        return field instanceof FlattenedKeyEsField;
    }
}
