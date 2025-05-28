/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.querydsl.query.SingleValueMatchQuery;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

/**
 * Lucene query that wraps another query and only selects documents that match
 * the wrapped query <strong>and</strong> have a single field value.
 * <p>
 *     This allows us to wrap regular lucene queries to have ESQL style semantics
 *     which will allow us to continue to push expressions to Lucene.
 * </p>
 * <p>
 *     We could have chosen not to wrap the lucene query and instead double check
 *     the results after they are loaded. That could be faster in some cases, but
 *     for now we're going to always wrap so we can always push. When we find cases
 *     where double checking is better we'll try that.
 * </p>
 * <p>
 *     NOTE: This will only work with {@code text} fields.
 * </p>
 */
public class SingleValueQuery extends Query {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "esql_single_value",
        Builder::new
    );

    private final Query next;
    private final String field;
    private final UseSyntheticSourceDelegate useSyntheticSourceDelegate;

    /**
     * Build.
     * @param next the query whose documents we should use for single-valued fields
     * @param field the name of the field whose values to check
     * @param useSyntheticSourceDelegate Should we check the field's synthetic source delegate (true)
     *                                   or it's values itself? If the field is a {@code text} field
     *                                   we often want to use its delegate.
     */
    public SingleValueQuery(Query next, String field, boolean useSyntheticSourceDelegate) {
        this(next, field, useSyntheticSourceDelegate ? UseSyntheticSourceDelegate.YES : UseSyntheticSourceDelegate.NO);
    }

    public SingleValueQuery(Query next, String field, UseSyntheticSourceDelegate useSyntheticSourceDelegate) {
        super(next.source());
        this.next = next;
        this.field = field;
        this.useSyntheticSourceDelegate = useSyntheticSourceDelegate;
    }

    @Override
    protected AbstractBuilder asBuilder() {
        return switch (useSyntheticSourceDelegate) {
            case NO -> new Builder(next.toQueryBuilder(), field, next.source());
            case YES -> new SyntheticSourceDelegateBuilder(next.toQueryBuilder(), field, next.source());
            case YES_NEGATED -> new NegatedSyntheticSourceDelegateBuilder(next.toQueryBuilder(), field, next.source());
        };
    }

    @Override
    protected String innerToString() {
        return next.toString();
    }

    @Override
    public SingleValueQuery negate(Source source) {
        return new SingleValueQuery(next.negate(source), field, switch (useSyntheticSourceDelegate) {
            case NO -> UseSyntheticSourceDelegate.NO;
            case YES -> UseSyntheticSourceDelegate.YES_NEGATED;
            case YES_NEGATED -> UseSyntheticSourceDelegate.YES;
        });
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass() || false == super.equals(o)) {
            return false;
        }
        SingleValueQuery other = (SingleValueQuery) o;
        return Objects.equals(next, other.next)
            && Objects.equals(field, other.field)
            && useSyntheticSourceDelegate == other.useSyntheticSourceDelegate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), next, field, useSyntheticSourceDelegate);
    }

    public abstract static class AbstractBuilder extends AbstractQueryBuilder<AbstractBuilder> {
        private final QueryBuilder next;
        private final String field;
        private final Source source;

        AbstractBuilder(QueryBuilder next, String field, Source source) {
            this.next = next;
            this.field = field;
            this.source = source;
        }

        AbstractBuilder(StreamInput in) throws IOException {
            super(in);
            this.next = in.readNamedWriteable(QueryBuilder.class);
            this.field = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                if (in instanceof PlanStreamInput psi) {
                    this.source = Source.readFrom(psi);
                } else {
                    /*
                     * For things like CanMatchNodeRequest we serialize without the Source. But we
                     * don't use it, so that's ok.
                     */
                    this.source = Source.readEmpty(in);
                }
            } else if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                this.source = readOldSource(in);
            } else {
                this.source = Source.EMPTY;
            }
        }

        @Override
        protected final void doWriteTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(next);
            out.writeString(field);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                source.writeTo(out);
            } else if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                writeOldSource(out, source);
            }
        }

        public QueryBuilder next() {
            return next;
        }

        public String field() {
            return field;
        }

        public Source source() {
            return source;
        }

        protected abstract AbstractBuilder rewrite(QueryBuilder next);

        @Override
        protected final QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
            QueryBuilder rewritten = next.rewrite(queryRewriteContext);
            if (rewritten instanceof MatchNoneQueryBuilder) {
                return rewritten;
            }
            if (rewritten == next) {
                return this;
            }
            return rewrite(rewritten);
        }

        @Override
        protected final boolean doEquals(AbstractBuilder other) {
            return next.equals(other.next) && field.equals(other.field);
        }

        @Override
        protected final int doHashCode() {
            return Objects.hash(next, field);
        }

        protected final org.apache.lucene.search.Query simple(MappedFieldType ft, SearchExecutionContext context) throws IOException {
            SingleValueMatchQuery singleValueQuery = new SingleValueMatchQuery(
                context.getForField(ft, MappedFieldType.FielddataOperation.SEARCH),
                Warnings.createWarnings(
                    DriverContext.WarningsMode.COLLECT,
                    source().source().getLineNumber(),
                    source().source().getColumnNumber(),
                    source().text()
                )
            );
            org.apache.lucene.search.Query rewrite = singleValueQuery.rewrite(context.searcher());
            if (rewrite instanceof MatchAllDocsQuery) {
                // nothing to filter
                return next().toQuery(context);
            }
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(next().toQuery(context), BooleanClause.Occur.FILTER);
            builder.add(rewrite, BooleanClause.Occur.FILTER);
            return builder.build();
        }
    }

    /**
     * Builds a {@code bool} query combining the "next" query and a {@link SingleValueMatchQuery}.
     */
    public static class Builder extends AbstractBuilder {
        Builder(QueryBuilder next, String field, Source source) {
            super(next, field, source);
        }

        Builder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(ENTRY.name);
            builder.field("field", field());
            builder.field("next", next(), params);
            builder.field("source", source().toString());
            builder.endObject();
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_11_X; // the first version of ESQL
        }

        @Override
        protected final org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) throws IOException {
            MappedFieldType ft = context.getFieldType(field());
            if (ft == null) {
                return new MatchNoDocsQuery("missing field [" + field() + "]");
            }
            return simple(ft, context);
        }

        @Override
        protected AbstractBuilder rewrite(QueryBuilder next) {
            return new Builder(next, field(), source());
        }
    }

    /**
     * Builds a {@code bool} query ANDing the "next" query, a {@link SingleValueMatchQuery},
     * and a {@link TermQuery} making sure we didn't ignore any values. Three total queries.
     * This is only used if the "next" query matches fields that would not be ignored. Read all
     * the paragraphs below to understand it. It's tricky!
     * <p>
     *     This is used in the case when you do {@code text_field == "foo"} and {@code text_field}
     *     has a {@code keyword} sub-field. See, {@code text} typed fields can't do our equality -
     *     they only do matching. But {@code keyword} fields *can* do the equality. In this case
     *     the "next" query is a {@link TermQuery} like {@code text_field.raw:foo}.
     * </p>
     * <p>
     *     But there's a big wrinkle! If you index a field longer than {@code ignore_above} into
     *     {@code text_field.raw} field then it'll drop its value on the floor. So the
     *     {@link SingleValueMatchQuery} isn't enough to emulate {@code ==}. You have to remove
     *     any matches that ignored a field. Luckily we have {@link IgnoredFieldMapper}! We can
     *     do a {@link TermQuery} like {@code NOT(_ignored:text_field.raw)} to filter those out.
     * </p>
     * <p>
     *     You may be asking, "how would the first {@code text_field.raw:foo} query work if the
     *     value we're searching for is very long?" In that case we never use this query at all.
     *     We have to delegate the filtering to the compute engine. No fancy lucene searches in
     *     that case.
     * </p>
     */
    public static class SyntheticSourceDelegateBuilder extends AbstractBuilder {
        SyntheticSourceDelegateBuilder(QueryBuilder next, String field, Source source) {
            super(next, field, source);
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("Not serialized");
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(ENTRY.name);
            builder.field("field", field() + ":synthetic_source_delegate");
            builder.field("next", next(), params);
            builder.field("source", source().toString());
            builder.endObject();
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            throw new UnsupportedOperationException("Not serialized");
        }

        @Override
        protected final org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) throws IOException {
            MappedFieldType ft = context.getFieldType(field());
            if (ft == null) {
                return new MatchNoDocsQuery("missing field [" + field() + "]");
            }
            ft = ((TextFieldMapper.TextFieldType) ft).syntheticSourceDelegate();

            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(next().toQuery(context), BooleanClause.Occur.FILTER);

            org.apache.lucene.search.Query singleValueQuery = new SingleValueMatchQuery(
                context.getForField(ft, MappedFieldType.FielddataOperation.SEARCH),
                Warnings.createWarnings(
                    DriverContext.WarningsMode.COLLECT,
                    source().source().getLineNumber(),
                    source().source().getColumnNumber(),
                    source().text()
                )
            );
            singleValueQuery = singleValueQuery.rewrite(context.searcher());
            if (singleValueQuery instanceof MatchAllDocsQuery == false) {
                builder.add(singleValueQuery, BooleanClause.Occur.FILTER);
            }

            org.apache.lucene.search.Query ignored = new TermQuery(new org.apache.lucene.index.Term(IgnoredFieldMapper.NAME, ft.name()));
            ignored = ignored.rewrite(context.searcher());
            if (ignored instanceof MatchNoDocsQuery == false) {
                builder.add(ignored, BooleanClause.Occur.MUST_NOT);
            }

            return builder.build();
        }

        @Override
        protected AbstractBuilder rewrite(QueryBuilder next) {
            return new Builder(next, field(), source());
        }
    }

    /**
     * Builds a query matching either ignored values OR the union of {@code next} query
     * and {@link SingleValueMatchQuery}. Three total queries. This is used to generate
     * candidate matches for queries like {@code NOT(a == "b")} where some values of {@code a}
     * are not indexed. In fact, let's use that as an example.
     * <p>
     *     In that case you use a query for {@code a != "b"} as the "next" query. Then
     *     this query will find all documents where {@code a} is single valued and
     *     {@code == "b"} AND all documents that have ignored some values of {@code a}.
     *     This produces <strong>candidate</strong> matches for {@code NOT(a == "b")}.
     *     It'll find documents like:
     * </p>
     * <ul>
     *     <li>"a"</li>
     *     <li>ignored_value</li>
     *     <li>["a", ignored_value]</li>
     *     <li>[ignored_value1, ignored_value2]</li>
     *     <li>["b", ignored_field]</li>
     * </ul>
     * <p>
     *     The first and second of those <strong>should</strong> match {@code NOT(a == "b")}.
     *     The last three should be rejected. So! When using this query you <strong>must</strong>
     *     push this query to the {@link LuceneSourceOperator} <strong>and</strong>
     *     retain it in the {@link FilterOperator}.
     * </p>
     * <p>
     *     This will not find:
     * </p>
     * <ul>
     *     <li>"b"</li>
     * </ul>
     * <p>
     *     And that's also great! These can't match {@code NOT(a == "b")}
     * </p>
     */
    public static class NegatedSyntheticSourceDelegateBuilder extends AbstractBuilder {
        NegatedSyntheticSourceDelegateBuilder(QueryBuilder next, String field, Source source) {
            super(next, field, source);
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("Not serialized");
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("negated_" + ENTRY.name);
            builder.field("field", field() + ":synthetic_source_delegate");
            builder.field("next", next(), params);
            builder.field("source", source().toString());
            builder.endObject();
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            throw new UnsupportedOperationException("Not serialized");
        }

        @Override
        protected final org.apache.lucene.search.Query doToQuery(SearchExecutionContext context) throws IOException {
            MappedFieldType ft = context.getFieldType(field());
            if (ft == null) {
                return new MatchNoDocsQuery("missing field [" + field() + "]");
            }
            ft = ((TextFieldMapper.TextFieldType) ft).syntheticSourceDelegate();
            org.apache.lucene.search.Query svNext = simple(ft, context);

            org.apache.lucene.search.Query ignored = new TermQuery(new org.apache.lucene.index.Term(IgnoredFieldMapper.NAME, ft.name()));
            ignored = ignored.rewrite(context.searcher());
            if (ignored instanceof MatchNoDocsQuery) {
                return svNext;
            }

            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(svNext, BooleanClause.Occur.SHOULD);
            builder.add(ignored, BooleanClause.Occur.SHOULD);
            return builder.build();
        }

        @Override
        protected AbstractBuilder rewrite(QueryBuilder next) {
            return new Builder(next, field(), source());
        }
    }

    /**
     * Write a {@link Source} including the text in it.
     */
    static void writeOldSource(StreamOutput out, Source source) throws IOException {
        out.writeInt(source.source().getLineNumber());
        out.writeInt(source.source().getColumnNumber());
        out.writeString(source.text());
    }

    /**
     * Read a {@link Source} including the text in it.
     */
    static Source readOldSource(StreamInput in) throws IOException {
        int line = in.readInt();
        int column = in.readInt();
        int charPositionInLine = column - 1;

        String text = in.readString();
        return new Source(new Location(line, charPositionInLine), text);
    }

    public enum UseSyntheticSourceDelegate {
        NO,
        YES,
        YES_NEGATED;
    }
}
