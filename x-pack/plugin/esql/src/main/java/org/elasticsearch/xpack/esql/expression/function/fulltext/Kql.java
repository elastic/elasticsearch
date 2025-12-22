/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExpressionContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.KqlQuery;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Map.entry;
import static org.elasticsearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.resolveTypeQuery;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.CASE_INSENSITIVE_FIELD;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.DEFAULT_FIELD_FIELD;
import static org.elasticsearch.xpack.kql.query.KqlQueryBuilder.TIME_ZONE_FIELD;

/**
 * Full text function that performs a {@link KqlQuery} .
 */
public class Kql extends FullTextFunction implements OptionalArgument, ConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Kql", Kql::readFrom);

    // Options for KQL function. They don't need to be serialized as the data nodes will retrieve them from the query builder
    private final transient Expression options;

    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(BOOST_FIELD.getPreferredName(), FLOAT),
        entry(CASE_INSENSITIVE_FIELD.getPreferredName(), BOOLEAN),
        entry(TIME_ZONE_FIELD.getPreferredName(), KEYWORD),
        entry(DEFAULT_FIELD_FIELD.getPreferredName(), KEYWORD)
    );

    @FunctionInfo(
        returnType = "boolean",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.0.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.1.0") },
        description = "Performs a KQL query. Returns true if the provided KQL query string matches the row.",
        examples = {
            @Example(file = "kql-function", tag = "kql-with-field", description = "Use KQL to filter by a specific field value"),
            @Example(
                file = "kql-function",
                tag = "kql-with-options",
                description = "Use KQL with additional options for case-insensitive matching and custom settings",
                applies_to = "stack: ga 9.3.0"
            ) }
    )
    public Kql(
        Source source,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Query string in KQL query string format."
        ) Expression queryString,
        @MapParam(
            name = "options",
            description = "(Optional) KQL additional options as <<esql-function-named-params,function named parameters>>."
                + " Available in stack version 9.3.0 and later.",
            params = {
                @MapParam.MapParamEntry(
                    name = "case_insensitive",
                    type = "boolean",
                    valueHint = { "true", "false" },
                    description = "If true, performs case-insensitive matching for keyword fields. Defaults to false."
                ),
                @MapParam.MapParamEntry(
                    name = "time_zone",
                    type = "keyword",
                    valueHint = { "UTC", "Europe/Paris", "America/New_York" },
                    description = "UTC offset or IANA time zone used to interpret date literals in the query string."
                ),
                @MapParam.MapParamEntry(
                    name = "default_field",
                    type = "keyword",
                    valueHint = { "*", "logs.*", "title" },
                    description = "Default field to search if no field is provided in the query string. Supports wildcards (*)."
                ),
                @MapParam.MapParamEntry(
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query. Defaults to 1.0."
                ) },
            optional = true
        ) Expression options
    ) {
        this(source, queryString, options, null);
    }

    public Kql(Source source, Expression queryString, Expression options, QueryBuilder queryBuilder) {
        super(source, queryString, options == null ? List.of(queryString) : List.of(queryString, options), queryBuilder);
        this.options = options;
    }

    private static Kql readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        // Options are not serialized - they're embedded in the QueryBuilder
        return new Kql(source, query, null, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query());
        out.writeOptionalNamedWriteable(queryBuilder());
        // Options are not serialized - they're embedded in the QueryBuilder
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression options() {
        return options;
    }

    private TypeResolution resolveQuery() {
        TypeResolution result = isType(query(), t -> t == KEYWORD || t == TEXT, sourceText(), FIRST, "keyword, text").and(
            isNotNull(query(), sourceText(), FIRST)
        );
        if (result.unresolved()) {
            return result;
        }
        result = resolveTypeQuery(query(), sourceText(), forPreOptimizationValidation(query()));
        if (result.equals(TypeResolution.TYPE_RESOLVED) == false) {
            return result;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected TypeResolution resolveParams() {
        return resolveQuery().and(Options.resolve(options(), source(), SECOND, ALLOWED_OPTIONS));
    }

    private Map<String, Object> kqlQueryOptions(ExpressionContext ctx) throws InvalidArgumentException {
        if (options() == null && ctx.configuration().zoneId().equals(ZoneOffset.UTC)) {
            return null;
        }

        Map<String, Object> kqlOptions = new HashMap<>();
        if (options() != null) {
            Options.populateMap((MapExpression) options(), kqlOptions, source(), SECOND, ALLOWED_OPTIONS);
        }
        kqlOptions.putIfAbsent(TIME_ZONE_FIELD.getPreferredName(), ctx.configuration().zoneId().getId());
        return kqlOptions;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Kql(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null, queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Kql::new, query(), options(), queryBuilder());
    }

    @Override
    protected Query translate(ExpressionContext ctx, LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return new KqlQuery(source(), Foldables.queryAsString(query(), sourceText()), kqlQueryOptions(ctx));
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Kql(source(), query(), options(), queryBuilder);
    }

    @Override
    public boolean equals(Object o) {
        // KQL does not serialize options, as they get included in the query builder. We need to override equals and hashcode to
        // ignore options when comparing.
        if (o == null || getClass() != o.getClass()) return false;
        var kql = (Kql) o;
        return Objects.equals(query(), kql.query()) && Objects.equals(queryBuilder(), kql.queryBuilder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(query(), queryBuilder());
    }

    @Override
    public String toString() {
        return "Kql{" + "query=" + query() + (options == null ? "" : ", options=" + options) + '}';
    }
}
