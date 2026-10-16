/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.BoostQuery;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.plugin.EsqlSearchExecutionContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A query over {@code keyword} fields that some shards may not map but may hold as unmapped fields, written as if every shard
 * mapped them as indexed {@code keyword} fields with doc values. Each shard runs {@code query} if, for every field, it maps the
 * field that way, keeps it in its {@code _unmapped} field, or cannot hold a value for it outside its mapping, and matches every
 * document otherwise. The filter the query is pushed from checks every document it
 * matches, so the shard can always fall back to matching everything.
 */
public class UnmappedFieldsQueryBuilder extends AbstractQueryBuilder<UnmappedFieldsQueryBuilder> {
    private static final TransportVersion ESQL_SHARD_RESOLVED_FIELDS = TransportVersion.fromName("esql_shard_resolved_fields");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "esql_unmapped_fields",
        UnmappedFieldsQueryBuilder::new
    );

    private final List<String> fields;
    private final QueryBuilder query;

    public UnmappedFieldsQueryBuilder(List<String> fields, QueryBuilder query) {
        this.fields = List.copyOf(fields);
        this.query = Objects.requireNonNull(query);
    }

    UnmappedFieldsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.fields = in.readStringCollectionAsImmutableList();
        this.query = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeStringCollection(fields);
        out.writeNamedWriteable(query);
    }

    public List<String> fields() {
        return fields;
    }

    public QueryBuilder query() {
        return query;
    }

    @Override
    protected org.apache.lucene.search.Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor)
        throws IOException {
        Map<String, MappedFieldType> inContainer = new HashMap<>();
        for (String field : fields) {
            boolean runs;
            if (context.isMappedField(field)) {
                runs = isPlainKeyword(context.getFieldType(field));
            } else if (context.unmappedFields().mayExist(field)) {
                MappedFieldType key = context instanceof EsqlSearchExecutionContext esql ? esql.unmappedFieldInContainer(field) : null;
                runs = key != null;
                if (runs) {
                    inContainer.put(field, key);
                }
            } else {
                runs = true;
            }
            if (runs == false) {
                // Scores nothing, like the filter it stands for
                org.apache.lucene.search.Query all = new BoostQuery(Queries.ALL_DOCS_INSTANCE, 0f);
                all.visit(visitor);
                return all;
            }
        }
        SearchExecutionContext target = inContainer.isEmpty()
            ? context
            : ((EsqlSearchExecutionContext) context).withUnmappedFields(inContainer);
        return Rewriteable.rewrite(query, target, true).toQuery(target, visitor);
    }

    /**
     * Whether {@code fieldType} answers {@code query} the way the filter does: an indexed {@code keyword} with doc values and no
     * normalizer, or a key of a {@code flattened} field.
     */
    private static boolean isPlainKeyword(MappedFieldType fieldType) {
        boolean keyword = (fieldType instanceof KeywordFieldMapper.KeywordFieldType kwd && kwd.hasNormalizer() == false)
            || fieldType instanceof FlattenedFieldMapper.KeyedFlattenedFieldType;
        return keyword && fieldType.isSearchable() && fieldType.hasDocValues();
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // Only doToQuery knows where each field lives: rewriting query here would turn a field held as an unmapped field into
        // match_none
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ENTRY.name);
        builder.field("fields", fields);
        builder.field("query", query, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected boolean doEquals(UnmappedFieldsQueryBuilder other) {
        return fields.equals(other.fields) && query.equals(other.query);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fields, query);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ESQL_SHARD_RESOLVED_FIELDS;
    }
}
