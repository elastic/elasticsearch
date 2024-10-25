/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FOURTH;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class Match extends FullTextFunction implements Validatable, TwoOptionalArguments {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Match", Match::readFrom);

    private final Expression field;
    private final Expression boost;
    private final Expression fuzziness;
    private final boolean isOperator;

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a match query on the specified field. Returns true if the provided query matches the row.",
        examples = { @Example(file = "match-function", tag = "match-with-field") }
    )
    public Match(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "Field that the query will target.") Expression field,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Text you wish to find in the provided field."
        ) Expression matchQuery,
        @Param(
            optional = true,
            name = "boost",
            type = { "integer", "double" },
            description = "Boost value for the query."
        ) Expression boost,
        @Param(optional = true, name = "boost", type = { "integer", "keyword" }, description = "Query fuzziness") Expression fuzziness
    ) {
        this(source, field, matchQuery, boost, fuzziness, false);
    }

    private Match(Source source, Expression field, Expression matchQuery, Expression boost, Expression fuzziness, boolean isOperator) {
        super(source, matchQuery, expressionList(field, matchQuery, boost, fuzziness));
        this.field = field;
        this.boost = boost;
        this.fuzziness = fuzziness;
        this.isOperator = isOperator;
    }

    private static List<Expression> expressionList(Expression field, Expression matchQuery, Expression boost, Expression fuzziness) {
        List<Expression> list = new ArrayList<>(4);
        list.add(field);
        list.add(matchQuery);
        if (boost != null) {
            list.add(boost);
        }
        if (fuzziness != null) {
            list.add(fuzziness);
        }
        return list;
    }

    public static Match operator(Source source, Expression field, Expression matchQuery, Expression boost, Expression fuzziness) {
        return new Match(source, field, matchQuery, boost, fuzziness, true);
    }

    private static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        boolean isOperator = false;
        Expression boost = null;
        Expression fuzziness = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.MATCH_OPERATOR_FUZZINESS_BOOSTING)) {
            boost = in.readOptionalNamedWriteable(Expression.class);
            fuzziness = in.readOptionalNamedWriteable(Expression.class);
            isOperator = in.readBoolean();
        }
        return new Match(source, field, query, boost, fuzziness, isOperator);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field());
        out.writeNamedWriteable(query());
        if (out.getTransportVersion().onOrAfter(TransportVersions.MATCH_OPERATOR_FUZZINESS_BOOSTING)) {
            out.writeOptionalNamedWriteable(boost);
            out.writeOptionalNamedWriteable(fuzziness);
            out.writeBoolean(isOperator);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveNonQueryParamTypes() {
        TypeResolution typeResolution = isNotNull(field, sourceText(), FIRST).and(isString(field, sourceText(), FIRST))
            .and(super.resolveNonQueryParamTypes());
        if (boost != null) {
            typeResolution = typeResolution.and(isNotNull(boost, sourceText(), THIRD).and(isNumeric(boost, sourceText(), THIRD)));
        }
        if (fuzziness != null) {
            typeResolution = typeResolution.and(
                isNotNull(fuzziness, sourceText(), FOURTH).and(
                    isType(fuzziness, dt -> dt == DataType.INTEGER || dt == DataType.KEYWORD, sourceText(), FOURTH, "integer,keyword")
                )
            );
        }
        return typeResolution;
    }

    @Override
    public void validate(Failures failures) {
        if (field instanceof FieldAttribute == false) {
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

        if (boost != null && boost.foldable() == false) {
            failures.add(
                Failure.fail(
                    field,
                    "[{}] {} boost must be evaluated to a constant. Value [{}] can't be resolved to a constant",
                    functionName(),
                    functionType(),
                    boost.sourceText()
                )
            );
        }

        if (fuzziness != null) {
            if (fuzziness.foldable() == false) {
                failures.add(
                    Failure.fail(
                        field,
                        "[{}] {} fuzziness must be evaluated to a constant. Value [{}] can't be resolved to a constant",
                        functionName(),
                        functionType(),
                        fuzziness.sourceText()
                    )
                );
            } else {
                try {
                    fuzziness();
                } catch (IllegalArgumentException | ElasticsearchParseException e) {
                    failures.add(
                        Failure.fail(
                            field,
                            "Invalid fuzziness value [{}] for [{}] {}",
                            fuzziness.sourceText(),
                            functionName(),
                            functionType()
                        )
                    );
                }
            }
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        Expression boost = newChildren.size() > 2 ? newChildren.get(2) : null;
        Expression fuzziness = newChildren.size() > 3 ? newChildren.get(3) : null;
        return new Match(source(), newChildren.get(0), newChildren.get(1), boost, fuzziness, isOperator);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Match::new, field, query(), boost, fuzziness);
    }

    protected TypeResolutions.ParamOrdinal queryParamOrdinal() {
        return SECOND;
    }

    public Expression field() {
        return field;
    }

    public Double boost() {
        if (boost == null) {
            return null;
        }
        return (Double) DataTypeConverter.convert(boost.fold(), DataType.DOUBLE);
    }

    public Fuzziness fuzziness() {
        if (fuzziness == null) {
            return null;
        }

        Object fuzinessAsObject = fuzziness.fold();
        if (fuzinessAsObject instanceof String stringValue) {
            return Fuzziness.fromString(stringValue);
        } else if (fuzinessAsObject instanceof BytesRef bytesRefValue) {
            return Fuzziness.fromString(bytesRefValue.utf8ToString());
        } else if (fuzinessAsObject instanceof Integer intValue) {
            return Fuzziness.fromEdits(intValue);
        }

        throw new IllegalArgumentException(
            format(null, "{} argument in {} {} needs to be resolved to a string or integer", THIRD, functionName(), functionType())
        );
    }

    @Override
    public String functionType() {
        return isOperator ? "operator" : super.functionType();
    }

    @Override
    public String functionName() {
        return isOperator ? ":" : super.functionName();
    }
}
