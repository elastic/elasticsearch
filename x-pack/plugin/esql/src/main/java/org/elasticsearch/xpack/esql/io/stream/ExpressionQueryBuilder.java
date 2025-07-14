/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MultiTermQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.io.IOException;
import java.util.Objects;

import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;

/**
 * Implements an Expression query builder, which matches documents based on a given expression.
 * The expression itself must provide the {@link TranslationAware#asLuceneQuery} interface to be translated into a Lucene query.
 * It allows for serialization of the expression and generate an AutomatonQuery on the data node
 * as Automaton does not support serialization.
 */
public class ExpressionQueryBuilder extends AbstractQueryBuilder<ExpressionQueryBuilder> implements MultiTermQueryBuilder {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "expressionQueryBuilder",
        ExpressionQueryBuilder::new
    );
    private final String fieldName;
    private final Expression expression;

    public ExpressionQueryBuilder(String fieldName, Expression expression) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (expression == null) {
            throw new IllegalArgumentException("expression cannot be null");
        }
        this.fieldName = fieldName;
        this.expression = expression;
    }

    /**
     * Read from a stream.
     */
    private ExpressionQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        assert in instanceof PlanStreamInput;
        this.expression = in.readNamedWriteable(Expression.class);
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(this.fieldName);
        assert out instanceof PlanStreamOutput;
        out.writeNamedWriteable(expression);
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ENTRY.name); // Use the appropriate query name
        builder.field("field", fieldName);
        builder.field("expression", expression.toString());
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        if (expression instanceof TranslationAware translationAware) {
            MappedFieldType fieldType = context.getFieldType(fieldName);
            if (fieldType == null) {
                return new MatchNoDocsQuery("Field [" + fieldName + "] does not exist");
            }
            return translationAware.asLuceneQuery(fieldType, CONSTANT_SCORE_REWRITE, context);
        } else {
            throw new UnsupportedOperationException("ExpressionQueryBuilder does not support non-automaton expressions");
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, expression);
    }

    @Override
    protected boolean doEquals(ExpressionQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(expression, other.expression);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        throw new UnsupportedOperationException("AutomatonQueryBuilder does not support getMinimalSupportedVersion");
    }
}
