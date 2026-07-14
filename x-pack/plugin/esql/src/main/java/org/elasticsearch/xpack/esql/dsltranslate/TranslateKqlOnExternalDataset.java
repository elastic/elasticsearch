/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.kql.parser.KqlParser;
import org.elasticsearch.xpack.kql.parser.KqlParsingException;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.CASE_INSENSITIVE_FIELD;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.DEFAULT_FIELD_FIELD;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.TIME_ZONE_FIELD;

/**
 * Makes {@code KQL()} apply to external datasets. A {@code KQL()} function pushes a Lucene query, which a dataset has no
 * scan to run — so the analyzer normally rejects it. This rule, over a {@code WHERE} whose only source is an
 * {@link ExternalRelation}, parses the (literal) KQL against the dataset's schema and translates it to an ES|QL
 * predicate via {@link QueryDslTranslator} — the same faithful, version-gated path the request filter uses.
 *
 * <p>It is deliberately fail-closed: a KQL query it cannot translate (a non-literal query, an unsupported construct like
 * a wildcard, a mixed index/dataset source, or an older cluster) is left as the {@code Kql} node, so the existing
 * verifier error fires — a user's {@code WHERE} must never silently stop filtering.
 */
public class TranslateKqlOnExternalDataset extends AnalyzerRules.ParameterizedAnalyzerRule<Filter, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        // The Filter is already resolved by the time Finish Analysis runs; the default (skip resolved) would no-op us.
        return false;
    }

    @Override
    protected LogicalPlan rule(Filter filter, AnalyzerContext context) {
        if (filter.condition().anyMatch(Kql.class::isInstance) == false) {
            return filter;
        }
        ExternalRelation relation = soleExternalRelationBelow(filter.child());
        if (relation == null) {
            return filter; // index / mixed / disallowed intermediate command — the verifier keeps today's behavior
        }
        if (context.minimumVersion().supports(RequestFilterGraft.ESQL_REQUEST_FILTER_ON_DATASET) == false) {
            return filter; // fail-closed on a mixed-version cluster: verifier error, never a plan an old node can't read
        }
        Expression rewritten = filter.condition().transformDown(Kql.class, kql -> translateOrKeep(kql, relation, context));
        return rewritten == filter.condition() ? filter : new Filter(filter.source(), filter.child(), rewritten);
    }

    /** The dataset analog of the verifier's index-side positional allow-list: the source is a single dataset leaf. */
    private static ExternalRelation soleExternalRelationBelow(LogicalPlan plan) {
        while (true) {
            if (plan instanceof ExternalRelation externalRelation) {
                return externalRelation;
            }
            if (plan instanceof Filter || plan instanceof OrderBy || plan instanceof Sample) {
                plan = ((UnaryPlan) plan).child();
                continue;
            }
            return null;
        }
    }

    private static Expression translateOrKeep(Kql kql, ExternalRelation relation, AnalyzerContext context) {
        // A non-literal or unresolved query is left as-is; the verifier then names the limitation.
        if (kql.resolved() == false || kql.query() instanceof Literal == false) {
            return kql;
        }
        Literal literal = (Literal) kql.query();
        if (literal.value() == null) {
            return kql;
        }
        String text = BytesRefs.toString(literal.value());

        Map<String, Object> options = new HashMap<>();
        if (kql.options() != null) {
            // Cannot throw: Kql.resolveParams already ran Options.resolve during Resolution and kql.resolved() gates above.
            Options.populateMap((MapExpression) kql.options(), options, kql.source(), SECOND, Kql.ALLOWED_OPTIONS);
        }
        boolean caseInsensitive = Boolean.TRUE.equals(options.get(CASE_INSENSITIVE_FIELD.getPreferredName()));
        String defaultField = (String) options.get(DEFAULT_FIELD_FIELD.getPreferredName());
        String explicitZone = (String) options.get(TIME_ZONE_FIELD.getPreferredName());
        ZoneId zone = explicitZone != null
            ? ZoneId.of(explicitZone)
            : QuerySettings.TIME_ZONE.get(context.configuration().resolvedSettings());
        // UTC is the translator's zone-naive parse default, so a redundant "UTC" range option would degrade every date
        // leaf for nothing — normalize it away. A real, non-UTC zone flows through and the translator rejects it.
        ZoneId effectiveZone = zone.normalized().equals(ZoneOffset.UTC) ? null : zone;

        // One source for the parser schema, the translator's fieldBinder, and its field-name set.
        Map<String, Attribute> byName = new HashMap<>();
        Map<String, DataType> schema = new HashMap<>();
        for (Attribute attribute : relation.output()) {
            byName.put(attribute.name(), attribute);
            schema.put(attribute.name(), attribute.dataType());
        }

        QueryBuilder dsl;
        try {
            dsl = new KqlParser().parseKqlQuery(text, new DatasetKqlParsingContext(schema, caseInsensitive, effectiveZone, defaultField));
        } catch (KqlParsingException e) {
            // A syntax error is the user's real problem — surface it, don't misreport it as a federation limitation.
            throw new VerificationException("Failed to parse KQL query [{}]: {}", text, e.getMessage());
        }

        QueryDslTranslator translator = new QueryDslTranslator(name -> {
            Attribute attribute = byName.get(name);
            return attribute != null ? attribute : Literal.NULL;
        }, byName.keySet(), context.configuration().absoluteStartedTimeInMillis());
        try {
            return translator.translate(dsl);
        } catch (TranslationUnsupportedException e) {
            return kql; // an untranslatable construct is left; the verifier fires (fail-closed)
        }
    }
}
