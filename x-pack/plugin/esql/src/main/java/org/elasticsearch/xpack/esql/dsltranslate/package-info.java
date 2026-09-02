/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Translates a Query DSL {@link org.elasticsearch.index.query.QueryBuilder} tree into an ES|QL
 * {@link org.elasticsearch.xpack.esql.core.expression.Expression} that means what the DSL means, so a request's
 * out-of-band {@code filter} can be applied to sources that have no Lucene scan of their own — external datasets today,
 * views tomorrow.
 *
 * <h2>Why this exists</h2>
 * An ES|QL request may carry a Query DSL {@code filter} beside the query text. On an index it is pushed into the scan.
 * On a dataset there is no such scan, so the filter used to be dropped. Rather than re-implement DSL matching against
 * every reader, we translate the filter once into the ES|QL expression that has the same meaning, and let the existing
 * planner and engine evaluate it — the translated filter is indistinguishable from a user-written {@code WHERE}.
 *
 * <h2>The pieces</h2>
 * <ul>
 *     <li>{@link org.elasticsearch.xpack.esql.dsltranslate.QueryDslTranslator} — the consumer-agnostic core. It knows
 *     nothing about datasets, requests, or the wire; it is given a {@code fieldBinder} (name &rarr; the expression that
 *     stands for that field on the source being translated against) and the query's {@code now}, and returns a boolean
 *     predicate over the supported subset: {@code bool}, {@code term}, {@code terms}, {@code range}, {@code exists},
 *     {@code match_all}, {@code match_none}, and the {@code match}/{@code match_phrase}/{@code multi_match} family as
 *     equality on an exact-typed field. Anything else — or a supported construct carrying an option it cannot honor
 *     faithfully — raises {@link org.elasticsearch.xpack.esql.dsltranslate.TranslationUnsupportedException} rather than
 *     silently mis-translating.</li>
 *     <li>{@link org.elasticsearch.xpack.esql.dsltranslate.FilterRewriter} — the source-agnostic <em>mechanism</em>.
 *     Given a target predicate, it installs the filter as an ordinary {@code Filter} above every node that predicate
 *     selects, binding the DSL against <em>that node's own</em> output schema (present field &rarr; its attribute,
 *     missing field &rarr; {@link org.elasticsearch.xpack.esql.core.expression.Literal#NULL}). It decides nothing about
 *     datasets, indices or views — reaching a new boundary is a change of the predicate, not of the mechanism.</li>
 *     <li>{@link org.elasticsearch.xpack.esql.dsltranslate.RequestFilterRewriter} — the dataset <em>policy</em> over
 *     that mechanism: it targets external leaves, and gates the rewrite. It is fail-closed: an unsupported clause fails
 *     the whole query with a 400 naming the construct, rather than silently applying a widened superset. It is feature-flagged
 *     (on in snapshot builds, excluded from release) and version-gated, because
 *     the translated predicate can contain expressions older nodes cannot deserialize.</li>
 * </ul>
 *
 * <h2>Two invariants the whole thing rests on</h2>
 * <ul>
 *     <li><b>Every emitted leaf is two-valued</b> (never returns {@code null}). Because a missing field binds to a null
 *     literal and every leaf folds it to {@code false}, plain {@code AND}/{@code OR}/{@code NOT} composition reproduces
 *     the DSL's missing-field leniency for free — a clause over a field the source lacks matches nothing under
 *     {@code AND} and everything under {@code NOT}, negation included.</li>
 *     <li><b>Literals take the field's type</b>, not the JSON value's. The translation runs after the analyzer, so
 *     nothing downstream inserts the implicit cast a user-written {@code WHERE} would get; the literal must already be
 *     the field's type or the evaluator is handed the wrong block.</li>
 * </ul>
 */
package org.elasticsearch.xpack.esql.dsltranslate;
