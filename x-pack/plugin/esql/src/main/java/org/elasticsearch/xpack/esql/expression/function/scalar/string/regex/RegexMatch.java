/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string.regex;

import org.apache.lucene.util.automaton.Automata;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.AbstractStringPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.AutomataMatch;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.io.IOException;

import static org.elasticsearch.TransportVersions.ESQL_REGEX_MATCH_WITH_CASE_INSENSITIVITY_8_19;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

abstract class RegexMatch<P extends AbstractStringPattern> extends org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch<
    P> implements EvaluatorMapper, TranslationAware.SingleValueTranslationAware {

    abstract String name();

    RegexMatch(Source source, Expression field, P pattern, boolean caseInsensitive) {
        super(source, field, pattern, caseInsensitive);
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }

    @Override
    public Boolean fold(FoldContext ctx) {
        return (Boolean) EvaluatorMapper.super.fold(source(), ctx);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return AutomataMatch.toEvaluator(
            source(),
            toEvaluator.apply(field()),
            // The empty pattern will accept the empty string
            pattern().pattern().isEmpty() ? Automata.makeEmptyString() : pattern().createAutomaton(caseInsensitive())
        );
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return pushdownPredicates.isPushableFieldAttribute(field()) ? Translatable.YES : Translatable.NO;
    }

    @Override
    public Expression singleValueField() {
        return field();
    }

    @Override
    public String nodeString() {
        return name() + "(" + field().nodeString() + ", \"" + pattern().pattern() + "\", " + caseInsensitive() + ")";
    }

    void serializeCaseInsensitivity(StreamOutput out) throws IOException {
        var transportVersion = out.getTransportVersion();
        if (transportVersion.onOrAfter(TransportVersions.ESQL_REGEX_MATCH_WITH_CASE_INSENSITIVITY)
            || transportVersion.isPatchFrom(ESQL_REGEX_MATCH_WITH_CASE_INSENSITIVITY_8_19)) {
            out.writeBoolean(caseInsensitive());
        } else if (caseInsensitive()) {
            // The plan has been optimized to run a case-insensitive match, which the remote peer cannot be notified of. Simply avoiding
            // the serialization of the boolean would result in wrong results.
            throw new EsqlIllegalArgumentException(
                name() + " with case insensitivity is not supported in peer node's version [{}]. Upgrade to version [{}] or newer.",
                out.getTransportVersion(),
                TransportVersions.ESQL_REGEX_MATCH_WITH_CASE_INSENSITIVITY
            );
        } // else: write nothing, the remote peer can execute the case-sensitive query
    }

    static boolean deserializeCaseInsensitivity(StreamInput in) throws IOException {
        var transportVersion = in.getTransportVersion();
        return (transportVersion.onOrAfter(TransportVersions.ESQL_REGEX_MATCH_WITH_CASE_INSENSITIVITY)
            || transportVersion.isPatchFrom(ESQL_REGEX_MATCH_WITH_CASE_INSENSITIVITY_8_19)) && in.readBoolean();
    }
}
