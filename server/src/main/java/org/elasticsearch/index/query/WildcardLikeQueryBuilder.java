/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Implements the wildcard search query. Supported wildcards are {@code *}, which
 * matches any character sequence (including the empty one), and {@code ?},
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term should not start with one of the wildcards {@code *} or
 * {@code ?}.
 */
public class WildcardLikeQueryBuilder extends AbstractQueryBuilder<WildcardLikeQueryBuilder> implements MultiTermQueryBuilder {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "wildcardLikeQueryBuilder",
        WildcardLikeQueryBuilder::new
    );

    private static final ParseField WILDCARD_FIELD = new ParseField("wildcard");
    private static final ParseField REWRITE_FIELD = new ParseField("rewrite");

    private final String fieldName;

    private final String value;

    private String rewrite;

    public static final boolean DEFAULT_CASE_INSENSITIVITY = false;
    private static final ParseField CASE_INSENSITIVE_FIELD = new ParseField("case_insensitive");
    private boolean caseInsensitive = DEFAULT_CASE_INSENSITIVITY;

    /**
     * Implements the wildcard search query. Supported wildcards are {@code *}, which
     * matches any character sequence (including the empty one), and {@code ?},
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards {@code *} or
     * {@code ?}.
     *
     * @param fieldName The field name
     * @param value The wildcard query string
     */
    public WildcardLikeQueryBuilder(String fieldName, String value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public WildcardLikeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readString();
        rewrite = in.readOptionalString();
        caseInsensitive = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeString(value);
        out.writeOptionalString(rewrite);
        out.writeBoolean(caseInsensitive);
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    public String value() {
        return value;
    }

    public WildcardLikeQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    public String rewrite() {
        return this.rewrite;
    }

    public WildcardLikeQueryBuilder caseInsensitive(boolean caseInsensitive) {
        this.caseInsensitive = caseInsensitive;
        return this;
    }

    public boolean caseInsensitive() {
        return this.caseInsensitive;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ENTRY.name);
        builder.startObject(fieldName);
        builder.field(WILDCARD_FIELD.getPreferredName(), value);
        if (rewrite != null) {
            builder.field(REWRITE_FIELD.getPreferredName(), rewrite);
        }
        if (caseInsensitive != DEFAULT_CASE_INSENSITIVITY) {
            builder.field(CASE_INSENSITIVE_FIELD.getPreferredName(), caseInsensitive);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    public static WildcardLikeQueryBuilder fromXContent(XContentParser parser) throws IOException {
        throw new UnsupportedOperationException("WildcardLikeQueryBuilder does not support parsing from XContent");
    }

    @Override
    protected QueryBuilder doIndexMetadataRewrite(QueryRewriteContext context) {
        MappedFieldType fieldType = context.getFieldType(this.fieldName);
        if (fieldType == null) {
            return new MatchNoneQueryBuilder("The \"" + getName() + "\" query is against a field that does not exist");
        }
        return maybeRewriteBasedOnConstantFields(fieldType, context);
    }

    @Override
    protected QueryBuilder doCoordinatorRewrite(CoordinatorRewriteContext coordinatorRewriteContext) {
        MappedFieldType fieldType = coordinatorRewriteContext.getFieldType(this.fieldName);
        // we don't rewrite a null field type to `match_none` on the coordinator because the coordinator has access
        // to only a subset of fields see {@link CoordinatorRewriteContext#getFieldType}
        return maybeRewriteBasedOnConstantFields(fieldType, coordinatorRewriteContext);
    }

    private QueryBuilder maybeRewriteBasedOnConstantFields(@Nullable MappedFieldType fieldType, QueryRewriteContext context) {
        if (fieldType instanceof ConstantFieldType constantFieldType) {
            // This logic is correct for all field types, but by only applying it to constant
            // fields we also have the guarantee that it doesn't perform I/O, which is important
            // since rewrites might happen on a network thread.
            Query query = constantFieldType.wildcardLikeQuery(value, caseInsensitive, context); // the rewrite method doesn't matter
            // Query query = constantFieldType.wildcardLikeQuery(value, null, caseInsensitive, context); // the rewrite method doesn't
            // matter
            if (query instanceof MatchAllDocsQuery) {
                return new MatchAllQueryBuilder();
            } else if (query instanceof MatchNoDocsQuery) {
                return new MatchNoneQueryBuilder("The \"" + getName() + "\" query was rewritten to a \"match_none\" query.");
            } else {
                assert false : "Constant fields must produce match-all or match-none queries, got " + query;
            }
        }
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);

        if (fieldType == null) {
            throw new IllegalStateException("Rewrite first");
        }

        MultiTermQuery.RewriteMethod method = QueryParsers.parseRewriteMethod(rewrite, null, LoggingDeprecationHandler.INSTANCE);
        return fieldType.wildcardLikeQuery(value, method, caseInsensitive, context);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, rewrite, caseInsensitive);
    }

    @Override
    protected boolean doEquals(WildcardLikeQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(value, other.value)
            && Objects.equals(rewrite, other.rewrite)
            && Objects.equals(caseInsensitive, other.caseInsensitive);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
