/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.MultiMatchQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.ANALYZER_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZINESS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.FUZZY_TRANSPOSITIONS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.GENERATE_SYNONYMS_PHRASE_QUERY;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.LENIENT_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.MAX_EXPANSIONS_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.MINIMUM_SHOULD_MATCH_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.OPERATOR_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.PREFIX_LENGTH_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.SLOP_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.TIE_BREAKER_FIELD;
import static org.elasticsearch.index.query.MultiMatchQueryBuilder.TYPE_FIELD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;

/**
 * Full text function that performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MultiMatchQuery} .
 */
public class MultiMatch extends FullTextFunction implements OptionalArgument, PostAnalysisPlanVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MultiMatch",
        MultiMatch::readFrom
    );

    public static final Map<String, DataType> OPTIONS = Map.ofEntries(
        entry(SLOP_FIELD.getPreferredName(), INTEGER),
        // TODO: add "zero_terms_query"
        entry(ANALYZER_FIELD.getPreferredName(), KEYWORD),
        entry(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), BOOLEAN),
        entry(FUZZINESS_FIELD.getPreferredName(), KEYWORD),
        entry(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), BOOLEAN),
        entry(LENIENT_FIELD.getPreferredName(), BOOLEAN),
        entry(MAX_EXPANSIONS_FIELD.getPreferredName(), INTEGER),
        entry(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), KEYWORD),
        entry(OPERATOR_FIELD.getPreferredName(), KEYWORD),
        entry(PREFIX_LENGTH_FIELD.getPreferredName(), INTEGER),
        entry(TIE_BREAKER_FIELD.getPreferredName(), FLOAT),
        entry(TYPE_FIELD.getPreferredName(), OBJECT)
    );

    @FunctionInfo(returnType = "boolean", preview = true, description = "something", examples = @Example(file = "eval", tag = "docsConcat"))

    // TODO: add annotations.
    // Fields are separate arguments like this: multi_match("harry potter", title, plot, summary, { "type": "cross_fields" })
    // Field1: title, Field2: plot, Field3: summary
    public MultiMatch(
        Source source,
        @Param(
            name = "query",
            type = { "keyword", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Value to find in the provided field."
        ) Expression query,
        List<Expression> fields,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "boost",
                    type = "float",
                    valueHint = { "2.5" },
                    description = "Floating point number used to decrease or increase the relevance scores of the query."
                ) },
            description = "description"
        ) Expression options
    ) {
        this(source, query, fields, options, null);
    }

    private final transient Expression options;

    // Need: "type" child, fields: array.
    // Other options: come as extra fields?
    private MultiMatch(Source source, Expression query, List<Expression> fields, Expression options, QueryBuilder queryBuilder) {
        super(source, query, Stream.concat(Stream.concat(Stream.of(query), fields.stream()), Stream.of(options)).toList(), queryBuilder);
        this.options = options;
    }

    private static MultiMatch readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression query = in.readNamedWriteable(Expression.class);
        List<Expression> fields = in.readNamedWriteableCollectionAsList(Expression.class);
        Expression options = in.readNamedWriteable(Expression.class);
        return new MultiMatch(source, query, fields, options);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().getFirst());
        out.writeNamedWriteableCollection(children().subList(1, children().size() - 1));
        out.writeNamedWriteable(children().getLast());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveParams() {
        // TODO
        return null;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MultiMatch(
            source(),
            newChildren.getFirst(),
            newChildren.subList(1, newChildren.size() - 1),
            newChildren.getLast(),
            queryBuilder()
        );
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(
            this,
            MultiMatch::new,
            children().getFirst(),
            children().subList(1, children().size() - 1),
            children().getLast()
        );
    }

    @Override
    protected Query translate(TranslatorHandler handler) {
        // TODO
        return new MultiMatchQuery(source(), Objects.toString(queryAsObject()), Map.of(), /*getOptions()*/ null);
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new MultiMatch(
            source(),
            children().getFirst(),
            children().subList(1, children().size() - 1),
            children().getLast(),
            queryBuilder
        );
    }

    public Expression options() {
        return options;
    }

    private Map<String, Object> getOptions() throws InvalidArgumentException {
        if (options() == null) {
            return null;
        }

        Map<String, Object> matchOptions = new HashMap<>();
        for (EntryExpression entry : ((MapExpression) options()).entryExpressions()) {
            Expression optionExpr = entry.key();
            Expression valueExpr = entry.value();
            TypeResolution resolution = isFoldable(optionExpr, sourceText(), SECOND).and(isFoldable(valueExpr, sourceText(), SECOND));
            if (resolution.unresolved()) {
                throw new InvalidArgumentException(resolution.message());
            }
            Object optionExprLiteral = ((Literal) optionExpr).value();
            Object valueExprLiteral = ((Literal) valueExpr).value();
            String optionName = optionExprLiteral instanceof BytesRef br ? br.utf8ToString() : optionExprLiteral.toString();
            String optionValue = valueExprLiteral instanceof BytesRef br ? br.utf8ToString() : valueExprLiteral.toString();
            // validate the optionExpr is supported
            DataType dataType = OPTIONS.get(optionName);
            if (dataType == null) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], expected one of {}", optionName, sourceText(), OPTIONS.keySet())
                );
            }
            try {
                matchOptions.put(optionName, DataTypeConverter.convert(optionValue, dataType));
            } catch (InvalidArgumentException e) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], {}", optionName, sourceText(), e.getMessage())
                );
            }
        }

        return matchOptions;
    }

    @Override
    public String functionName() {
        return ENTRY.name;
    }

}
