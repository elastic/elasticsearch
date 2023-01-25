/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper.requireNonNull;

public class SparseTermsQueryBuilder extends AbstractQueryBuilder<SparseTermsQueryBuilder> {
    public static final String NAME = "sparse_terms";

    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField SCORES = new ParseField("scores");
    private static final ParseField TERMS = new ParseField("terms");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SparseTermsQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "sparse_terms",
        false,
        a -> new SparseTermsQueryBuilder((String) a[0], (List<String>) a[1], (List<Float>) a[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), TERMS);
        PARSER.declareFloatArray(ConstructingObjectParser.constructorArg(), SCORES);
        AbstractQueryBuilder.declareStandardFields(PARSER);
    }

    private final List<String> terms;
    private final List<Float> scores;
    private final String fieldName;

    public SparseTermsQueryBuilder(String fieldName, List<String> terms, List<Float> scores) {
        this.fieldName = requireNonNull(fieldName, FIELD);
        this.terms = requireNonNull(terms, TERMS);
        this.scores = requireNonNull(scores, SCORES);
        if (this.scores.size() != this.terms.size()) {
            throw new IllegalArgumentException(format("[%s] length must equal [%s]", TERMS.getPreferredName(), SCORES.getPreferredName()));
        }
    }

    public SparseTermsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.terms = in.readStringList();
        this.scores = in.readList(StreamInput::readFloat);
        this.fieldName = in.readString();
    }

    public List<String> getTerms() {
        return terms;
    }

    public List<Float> getScores() {
        return scores;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringCollection(terms);
        out.writeCollection(scores, StreamOutput::writeFloat);
        out.writeString(fieldName);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(FIELD.getPreferredName(), fieldName);
        builder.field(SCORES.getPreferredName(), scores);
        builder.field(TERMS.getPreferredName(), terms);
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }
        if (fieldType.typeName().equals("rank_features") == false) {
            throw new QueryShardException(
                context,
                "[{}] can only query [rank_features] fields; found [{}] on index [{}]",
                NAME,
                fieldType.name(),
                context.index().getName()
            );
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(1);
        for (int i = 0; i < terms.size(); i++) {
            builder.add(new SparseTermsQuery(new Term(fieldName, terms.get(i)), scores.get(i)), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        SearchExecutionContext context = queryRewriteContext.convertToSearchExecutionContext();
        if (context != null) {
            MappedFieldType fieldType = context.getFieldType(fieldName);
            if (fieldType == null) {
                return new MatchNoneQueryBuilder();
            }
        }
        if (terms.isEmpty()) {
            return new MatchNoneQueryBuilder();
        }
        return super.doRewrite(context);
    }

    @Override
    protected boolean doEquals(SparseTermsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName) && Objects.equals(other.terms, terms) && Objects.equals(other.scores, scores);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, terms, scores);
    }

}
