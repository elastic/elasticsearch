/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal;
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
 * This class contains the common functionalities between the match function ({@link Match}) and match operator ({@link MatchOperator}),
 * so the two subclasses just contains the different code
 */
public abstract class AbstractMatchFullTextFunction extends FullTextFunction implements PostOptimizationVerificationAware {
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
    protected final Expression field;

    protected AbstractMatchFullTextFunction(
        Source source,
        Expression query,
        List<Expression> children,
        QueryBuilder queryBuilder,
        Expression field
    ) {
        super(source, query, children, queryBuilder);
        this.field = field;
    }

    public Expression field() {
        return field;
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

    protected ParamOrdinal queryParamOrdinal() {
        return SECOND;
    }

}
