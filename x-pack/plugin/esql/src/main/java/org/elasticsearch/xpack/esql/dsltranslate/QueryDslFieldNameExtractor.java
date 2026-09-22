/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Collects the field names a request {@code filter} references, so that pre-analysis can ask field-caps for them.
 *
 * <p>Needed because {@link ViewRequestFilterRewriter} evaluates the filter <em>in ES|QL</em>, above a view's output, and
 * so needs each referenced field to resolve to a real {@code Attribute}. Field-name pruning is computed from the query
 * text, which need not mention the filter's fields at all: for {@code FROM my_view | KEEP id} with a filter on
 * {@code region}, {@code region} is pruned away, the rewriter binds it to {@link Literal#NULL} (reproducing Query DSL's
 * missing-field leniency) and the filter silently matches nothing. Kibana makes this the common case rather than an edge
 * case — it discovers a view's filterable columns by running {@code FROM my_view | LIMIT 0} and builds filters from that
 * output schema, independently of whatever the panel query happens to select.
 *
 * <p>The plain-index path has no such problem: there the raw DSL is handed to Lucene, which resolves names against the
 * real mapping and is unaffected by what ES|QL loaded.
 *
 * <p>Extraction reuses {@link QueryDslTranslator} rather than walking the DSL separately, so the set of names can never
 * drift from the set of constructs the translator actually supports: every field-referencing construct resolves its
 * field through the {@code fieldBinder}, so a collecting binder sees exactly the referenced names.
 */
public final class QueryDslFieldNameExtractor {

    /**
     * @param fieldNames        the referenced field names; meaningful only when {@code requiresAllFields} is {@code false}
     * @param requiresAllFields {@code true} when the referenced names cannot be enumerated and the caller must fall back
     *                          to requesting every field
     */
    public record Result(Set<String> fieldNames, boolean requiresAllFields) {
        private static final Result ALL_FIELDS = new Result(Set.of(), true);
    }

    private QueryDslFieldNameExtractor() {}

    /**
     * Extracts the field names {@code filter} references.
     *
     * <p>Returns {@link Result#requiresAllFields()} when the names cannot be enumerated:
     * <ul>
     *   <li>a {@code multi_match}, whose fields are resolved by matching patterns against the source's <em>complete</em>
     *       field list — so its referenced fields are unknowable without that list, even when it names them explicitly</li>
     *   <li>a construct outside the supported subset, which leaves the collected set possibly incomplete. Such a filter
     *       fails the query fail-closed once it reaches a view, so over-requesting here costs nothing.</li>
     * </ul>
     */
    public static Result extract(QueryBuilder filter, Configuration configuration) {
        Set<String> collected = new HashSet<>();
        FieldListAccessRecorder fieldList = new FieldListAccessRecorder();
        QueryDslTranslator translator = new QueryDslTranslator(name -> {
            collected.add(name);
            // Bind to NULL: only the requested names matter here, not the expression that comes out. This mirrors what
            // the rewriter does for a field absent from the output, so translation stays on a supported path.
            return Literal.NULL;
        }, fieldList, configuration);

        QueryDslTranslator.TranslationResult result;
        try {
            result = translator.translate(filter);
        } catch (Exception e) {
            // Pre-analysis must not fail a query over a filter that may never reach a view; fall back instead. If it
            // does reach one, the rewriter reports the problem properly.
            return Result.ALL_FIELDS;
        }
        return (result.isComplete() && fieldList.accessed == false) ? new Result(collected, false) : Result.ALL_FIELDS;
    }

    /**
     * An empty field list that records whether it was read. {@link QueryDslTranslator} consults its field list only to
     * expand {@code multi_match} patterns, so any read means the translation depended on knowing every field. Recording
     * the access — rather than inspecting the DSL for {@code multi_match} ourselves — keeps this tied to the translator's
     * actual behaviour, so a future construct that needs the full list is handled conservatively by default.
     */
    private static final class FieldListAccessRecorder extends AbstractSet<String> {
        private boolean accessed;

        @Override
        public Iterator<String> iterator() {
            accessed = true;
            return Collections.emptyIterator();
        }

        @Override
        public int size() {
            return 0;
        }
    }
}
