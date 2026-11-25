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
 * One principle: <b>a key is a dotted name, and the source it comes from answers it.</b> Everything below follows from that. Keys work
 * like unmapped fields that are loaded ({@code SET unmapped_fields="load"}): the name belongs to its source, and the column flows up
 * from there.
 * <ol>
 *     <li><b>An index answers from its mapping.</b> A name that the query uses, that no index maps as a leaf, and that is under a
 *     {@code flattened} field of an index is a {@code keyword} field of that index, wherever the index comes from: {@code FROM}, a
 *     lookup index or an enrich policy. Each shard reads the name as its own mapping says, so in a union of indices the keys of one
 *     fill the column beside the mapped or loaded values of another.</li>
 *     <li><b>A subquery is transparent.</b> It is not a source; the sources inside it answer.</li>
 *     <li><b>A view is index-like and answers from its output.</b> The query reads a view as it would read an index that stores the
 *     view's output. So {@code attrs.foo} above a view is the view's column of that name if it has one, and otherwise the key
 *     {@code foo} of the view's {@code flattened} column {@code attrs}, read from the value with {@code FIELD_EXTRACT}. It never
 *     reaches fields that the view does not output. When several flattened columns of the view are above the name, the most
 *     specific one is read first. {@code unmapped_fields} stops at a view, so above it the name can only be a key.</li>
 *     <li><b>A key is a column like any other.</b> A key that the query names is a column of its source's output. A command replaces
 *     exactly the names it brings in, so {@code LOOKUP JOIN} and {@code ENRICH} bring in a key of their {@code flattened} fields only
 *     where no column of that name exists. This is also what keeps queries that are valid without keys unchanged.</li>
 *     <li><b>Nothing inside a query carries keys.</b> There is no parent and child: {@code RENAME} renames the column and not its keys,
 *     {@code EVAL} replaces the column and not its keys, and a {@code KEEP} that does not list a key drops it. After
 *     {@code RENAME attrs AS a} there is no {@code a.foo}, and {@code attrs.foo} is still answered by its source. To read a key from
 *     a value, use {@code FIELD_EXTRACT}.</li>
 *     <li><b>{@code unmapped_fields} decides only what no source answers.</b> A key that a source holds wins over {@code nullify} and
 *     {@code load} for that source. Where those modes made the name a {@code null} of the null type before keys were read, a query
 *     that only verifies with that null keeps that meaning, with a warning: a key is a {@code keyword}, and the null type was
 *     accepted by every expression.</li>
 *     <li><b>Patterns never match keys, and {@code DROP} reaches no key.</b> Removing the key from inside the value is tracked in
 *     elastic/elasticsearch#166913.</li>
 * </ol>
 * Another type that exposes names inside a value has to follow the same rules: its sources answer the names, and the analyzer uses the
 * same checks. A type that cannot be read from a value, such as {@code nested}, cannot be answered by a view unless the view outputs
 * the name as a column. Do not add another set of rules for it.
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
