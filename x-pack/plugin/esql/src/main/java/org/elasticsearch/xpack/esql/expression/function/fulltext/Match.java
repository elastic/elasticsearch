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
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.EsqlExpressionTranslators;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.index.query.MatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_REWRITE_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.MatchQueryBuilder.LENIENT_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MatchQueryBuilder.PREFIX_LENGTH_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
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
public class Match extends FullTextFunction implements Validatable, OptionalArgument {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);

    private final Expression field;

    private final Expression options;

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

    static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(
        entry(ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), BOOLEAN),
        entry(Fuzziness.FIELD.getPreferredName(), KEYWORD),
        entry(AbstractQueryBuilder.BOOST_FIELD.getPreferredName(), DataType.FLOAT),
        entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), DataType.BOOLEAN),
        entry(FUZZY_REWRITE_FIELD.getPreferredName(),  KEYWORD),
        entry(MatchQueryBuilder.LENIENT_FIELD.getPreferredName(), BOOLEAN),
        entry(MAX_EXPANSIONS_FIELD.getPreferredName(), DataType.INTEGER),
        entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), KEYWORD),
        entry(OPERATOR_FIELD.getPreferredName(), KEYWORD),
        entry(PREFIX_LENGTH_FIELD.getPreferredName(), DataType.INTEGER)
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = """
            Use `MATCH` to perform a <<query-dsl-match-query,match query>> on the specified field.
            Using `MATCH` is equivalent to using the `match` query in the Elasticsearch Query DSL.

            Match can be used on text fields, as well as other field types like boolean, dates, and numeric types.

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
        ) Expression matchQuery,
        @MapParam(
            params = { @MapParam.MapParamEntry(name = "options", valueHint = { "2", "2.0" }) },
            description = "Match additional options. See <<query-dsl-match-query,match query>> for more information.",
            optional = true
        ) Expression options
    ) {
        this(source, field, matchQuery, options,null);
    }

    public Match(Source source, Expression field, Expression matchQuery, Expression options, QueryBuilder queryBuilder) {
        super(source, matchQuery, List.of(field, matchQuery, options), queryBuilder);
        this.options = options;
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
        Expression options = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_MATCH_OPTIONS)) {
            options = in.readOptionalNamedWriteable(Expression.class);
        }
        return new Match(source, field, query, options, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(queryBuilder());
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_MATCH_OPTIONS)) {
            out.writeOptionalNamedWriteable(options());
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveNonQueryParamTypes() {
        TypeResolution resolution = isNotNull(field, sourceText(), FIRST).and(
            isType(
                field,
                FIELD_DATA_TYPES::contains,
                sourceText(),
                FIRST,
                "keyword, text, boolean, date, date_nanos, double, integer, ip, long, unsigned_long, version"
            )
        );
        if (resolution.unresolved()) {
            return resolution;
        }

        // TODO Extract common logic for validating options
        if (options() != null) {
            // MapExpression does not have a DataType associated with it
            resolution = isMapExpression(options(), sourceText(), SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }

            try {
                parseOptions();
            } catch (InvalidArgumentException e) {
                return new TypeResolution(e.getMessage());
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    private Map<String, Object> parseOptions() throws InvalidArgumentException {
        Map<String, Object> options = new HashMap<>();
        // Match is lenient by default to avoid failing on incompatible types
        options.put(LENIENT_FIELD.getPreferredName(), true);

        for (EntryExpression entry : ((MapExpression) options()).entryExpressions()) {
            Expression optionExpr = entry.key();
            Expression valueExpr = entry.value();
            TypeResolution resolution = isFoldable(optionExpr, sourceText(), SECOND).and(isFoldable(valueExpr, sourceText(), SECOND));
            if (resolution.unresolved()) {
                throw new InvalidArgumentException(resolution.message());
            }
            Object optionExprFold = optionExpr.fold();
            Object valueExprFold = valueExpr.fold();
            String optionName = optionExprFold instanceof BytesRef br ? br.utf8ToString() : optionExprFold.toString();
            String optionValue = valueExprFold instanceof BytesRef br ? br.utf8ToString() : valueExprFold.toString();
            // validate the optionExpr is supported
            DataType dataType = ALLOWED_OPTIONS.get(optionName);
            if (dataType == null) {
                throw new InvalidArgumentException(
                    format(
                        null,
                        "Invalid option [{}] in [{}], expected one of {}",
                        optionName,
                        sourceText(),
                        ALLOWED_OPTIONS.keySet()
                    )
                );
            }
            try {
                options.put(optionName, DataTypeConverter.convert(optionValue, dataType));
            } catch (InvalidArgumentException e) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], {}", optionName, sourceText(), e.getMessage())
                );
            }
        }

        return options;
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
    public void validate(Failures failures) {
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
        Object queryAsObject = query().fold();

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
        return new Match(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2), queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field, query(), options(), queryBuilder());
    }

    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return SECOND;
    }

    public Expression field() {
        return field;
    }

    private Expression options() {
        return options;
    }

    @Override
    public String functionType() {
        return isOperator() ? "operator" : super.functionType();
    }

    @Override
    protected ExpressionTranslator<Match> translator() {
        return new EsqlExpressionTranslators.MatchFunctionTranslator();
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new Match(source(), field, query(), options(), queryBuilder);
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

    public Map<String, Object> optionsMap() {
        return parseOptions();
    }
}
