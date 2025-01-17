/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison.formatIncompatibleTypesMessage;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class Match extends FullTextFunction implements PostOptimizationVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);

    private final Expression field;

    private transient Boolean isOperator;

    public static final Set<DataType> FIELD_DATA_TYPES = Set.of(
        KEYWORD,
        TEXT,
        SEMANTIC_TEXT,
        BOOLEAN,
        DATETIME,
        DATE_NANOS,
        DOUBLE,
        INTEGER,
        IP,
        LONG,
        UNSIGNED_LONG,
        VERSION
    );
    public static final Set<DataType> QUERY_DATA_TYPES = Set.of(
        KEYWORD,
        BOOLEAN,
        DATETIME,
        DATE_NANOS,
        DOUBLE,
        INTEGER,
        IP,
        LONG,
        UNSIGNED_LONG,
        VERSION
    );

    @FunctionInfo(
        returnType = "boolean",
        operator = ":",
        preview = true,
        description = """
            Use `MATCH` to perform a <<query-dsl-match-query,match query>> on the specified field.
            Using `MATCH` is equivalent to using the `match` query in the Elasticsearch Query DSL.

            Match can be used on fields from the text family like <<text, text>> and <<semantic-text, semantic_text>>,
            as well as other field types like keyword, boolean, dates, and numeric types.

            For a simplified syntax, you can use the <<esql-search-operators,match operator>> `:` operator instead of `MATCH`.

            `MATCH` returns true if the provided query matches the row.""",
        examples = { @Example(file = "match-function", tag = "match-with-field") }
    )
    public Match(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Field that the query will target."
        ) Expression field,
        @Param(
            name = "query",
            type = { "keyword", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Value to find in the provided field."
        ) Expression matchQuery
    ) {
        this(source, field, matchQuery, null);
    }

    public Match(Source source, Expression field, Expression matchQuery, QueryBuilder queryBuilder) {
        super(source, matchQuery, List.of(field, matchQuery), queryBuilder);
        this.field = field;
    }

    private static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        }
        return new Match(source, field, query, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(queryBuilder());
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveNonQueryParamTypes() {
        return isNotNull(field, sourceText(), FIRST).and(
            isType(
                field,
                FIELD_DATA_TYPES::contains,
                sourceText(),
                FIRST,
                "keyword, text, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
            )
        );
    }

    @Override
    protected TypeResolution resolveQueryParamType() {
        return isType(
            query(),
            QUERY_DATA_TYPES::contains,
            sourceText(),
            queryParamOrdinal(),
            "keyword, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
        ).and(isNotNullAndFoldable(query(), sourceText(), queryParamOrdinal()));
    }

    @Override
    protected TypeResolution checkParamCompatibility() {
        DataType fieldType = field().dataType();
        DataType queryType = query().dataType();

        // Field and query types should match. If the query is a string, then it can match any field type.
        if ((fieldType == queryType) || (queryType == KEYWORD)) {
            return TypeResolution.TYPE_RESOLVED;
        }

        if (fieldType.isNumeric() && queryType.isNumeric()) {
            // When doing an unsigned long query, field must be an unsigned long
            if ((queryType == UNSIGNED_LONG && fieldType != UNSIGNED_LONG) == false) {
                return TypeResolution.TYPE_RESOLVED;
            }
        }

        return new TypeResolution(formatIncompatibleTypesMessage(fieldType, queryType, sourceText()));
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        Expression fieldExpression = field();
        // Field may be converted to other data type (field_name :: data_type), so we need to check the original field
        if (fieldExpression instanceof AbstractConvertFunction convertFunction) {
            fieldExpression = convertFunction.field();
        }
        if (fieldExpression instanceof FieldAttribute == false) {
            failures.add(
                Failure.fail(
                    field,
                    "[{}] {} cannot operate on [{}], which is not a field from an index mapping",
                    functionName(),
                    functionType(),
                    field.sourceText()
                )
            );
        }
    }

    @Override
    public Object queryAsObject() {
        Object queryAsObject = query().fold(FoldContext.small() /* TODO remove me */);

        // Convert BytesRef to string for string-based values
        if (queryAsObject instanceof BytesRef bytesRef) {
            return switch (query().dataType()) {
                case IP -> EsqlDataTypeConverter.ipToString(bytesRef);
                case VERSION -> EsqlDataTypeConverter.versionToString(bytesRef);
                default -> bytesRef.utf8ToString();
            };
        }

        // Converts specific types to the correct type for the query
        if (query().dataType() == DataType.UNSIGNED_LONG) {
            return NumericUtils.unsignedLongAsBigInteger((Long) queryAsObject);
        } else if (query().dataType() == DataType.DATETIME && queryAsObject instanceof Long) {
            // When casting to date and datetime, we get a long back. But Match query needs a date string
            return EsqlDataTypeConverter.dateTimeToString((Long) queryAsObject);
        } else if (query().dataType() == DATE_NANOS && queryAsObject instanceof Long) {
            return EsqlDataTypeConverter.nanoTimeToString((Long) queryAsObject);
        }

        return queryAsObject;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Match(source(), newChildren.get(0), newChildren.get(1), queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field, query(), queryBuilder());
    }

    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return SECOND;
    }

    public Expression field() {
        return field;
    }

    @Override
    public String functionType() {
        return isOperator() ? "operator" : super.functionType();
    }

    @Override
    protected Query translate(TranslatorHandler handler) {
        Expression fieldExpression = field;
        // Field may be converted to other data type (field_name :: data_type), so we need to check the original field
        if (fieldExpression instanceof AbstractConvertFunction convertFunction) {
            fieldExpression = convertFunction.field();
        }
        if (fieldExpression instanceof FieldAttribute fieldAttribute) {
            String fieldName = fieldAttribute.name();
            if (fieldAttribute.field() instanceof MultiTypeEsField multiTypeEsField) {
                // If we have multiple field types, we allow the query to be done, but getting the underlying field name
                fieldName = multiTypeEsField.getName();
            }
            // Make query lenient so mixed field types can be queried when a field type is incompatible with the value provided
            return new MatchQuery(source(), fieldName, queryAsObject(), Map.of("lenient", "true"));
        }

        throw new IllegalArgumentException("Match must have a field attribute as the first argument");
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Match(source(), field, query(), queryBuilder);
    }

    @Override
    public String functionName() {
        return isOperator() ? ":" : super.functionName();
    }

    private boolean isOperator() {
        if (isOperator == null) {
            isOperator = source().text().toUpperCase(Locale.ROOT).matches("^" + super.functionName() + "\\s*\\(.*\\)") == false;
        }
        return isOperator;
    }
}
