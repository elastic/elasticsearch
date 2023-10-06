/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class RetrieverBuilder<RB extends RetrieverBuilder<?>> implements VersionedNamedWriteable, ToXContentObject, Rewriteable<RB> {

    public static final ParseField FROM_FIELD = new ParseField("from");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField FILTER_FIELD = new ParseField("filter");
    public static final ParseField _NAME_FIELD = new ParseField("_name");

    protected int from = SearchService.DEFAULT_FROM;
    protected int size = SearchService.DEFAULT_SIZE;
    protected QueryBuilder preFilterQueryBuilder;
    protected String _name;

    public RetrieverBuilder() {

    }

    public RetrieverBuilder(RetrieverBuilder<?> original) {
        from = original.from;
        size = original.size;
        preFilterQueryBuilder = original.preFilterQueryBuilder;
        _name = original._name;
    }

    public RetrieverBuilder(StreamInput in) throws IOException {
        from = in.readVInt();
        size = in.readVInt();
        preFilterQueryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        _name = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeOptionalNamedWriteable(preFilterQueryBuilder);
        out.writeOptionalString(_name);
        doWriteTo(out);
    }

    public abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FROM_FIELD.getPreferredName(), from);
        builder.field(SIZE_FIELD.getPreferredName(), size);
        if (preFilterQueryBuilder != null) {
            builder.field(FILTER_FIELD.getPreferredName(), preFilterQueryBuilder);
        }
        if (_name != null) {
            builder.field(_NAME_FIELD.getPreferredName(), _name);
        }
        doToXContent(builder, params);
        builder.endObject();

        return builder;
    }

    protected abstract void doToXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    @SuppressWarnings("unchecked")
    public RB rewrite(QueryRewriteContext ctx) throws IOException {
        if (preFilterQueryBuilder != null) {
            QueryBuilder rewrittenFilter = preFilterQueryBuilder.rewrite(ctx);

            if (rewrittenFilter != preFilterQueryBuilder) {
                return (RB) shallowCopyInstance().preFilterQueryBuilder(preFilterQueryBuilder);
            }
        }

        return (RB) this;
    }

    protected abstract RB shallowCopyInstance();

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        RB other = (RB) obj;
        return from == other.from && size == other.size && Objects.equals(preFilterQueryBuilder, other.preFilterQueryBuilder) && Objects.equals(_name, other._name) && doEquals(other);
    }

    protected abstract boolean doEquals(RB other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), from, size, preFilterQueryBuilder, _name, doHashCode());
    }

    protected abstract int doHashCode();

    public int from() {
        return from;
    }

    @SuppressWarnings("unchecked")
    public RB from(int from) {
        this.from = from;
        return (RB) this;
    }

    public int size() {
        return size;
    }

    @SuppressWarnings("unchecked")
    public RB size(int size) {
        this.size = size;
        return (RB) this;
    }

    public QueryBuilder preFilterQueryBuilder() {
        return preFilterQueryBuilder;
    }

    @SuppressWarnings("unchecked")
    public RB preFilterQueryBuilder(QueryBuilder preFilter) {
        this.preFilterQueryBuilder = preFilter;
        return (RB) this;
    }

    public String _name() {
        return _name;
    }

    @SuppressWarnings("unchecked")
    public RB _name(String _name) {
        this._name = _name;
        return (RB) this;
    }
}
