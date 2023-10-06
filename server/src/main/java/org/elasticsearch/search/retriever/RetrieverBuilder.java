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
    protected QueryBuilder filter;
    protected String _name;

    public RetrieverBuilder() {

    }

    public RetrieverBuilder(StreamInput in) throws IOException {
        from = in.readVInt();
        size = in.readVInt();
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        _name = in.readOptionalString();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(from);
        out.writeVInt(size);
        out.writeOptionalNamedWriteable(filter);
        out.writeOptionalString(_name);
        doWriteTo(out);
    }

    public abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FROM_FIELD.getPreferredName(), from);
        builder.field(SIZE_FIELD.getPreferredName(), size);
        if (filter != null) {
            builder.field(FILTER_FIELD.getPreferredName(), filter);
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
    public final RB rewrite(QueryRewriteContext ctx) throws IOException {
        RB rewrittenRetriever = doRewrite(ctx);
        QueryBuilder rewrittenFilter = filter;
        if (filter != null) {
            rewrittenFilter = filter.rewrite(ctx);
        }
        if (rewrittenFilter != filter) {
            if (rewrittenRetriever == this) {
                rewrittenRetriever = copy();
            }
        }
        rewrittenRetriever.filter = filter;
        return rewrittenRetriever;
    }

    protected abstract RB doRewrite(QueryRewriteContext ctx) throws IOException;

    public final RB copy() {
        return doCopy(null);
    }

    protected RB doCopy(RB copy) {
        Objects.requireNonNull(copy);
        copy.from = this.from;
        copy.size = this.size;
        copy.filter = this.filter;
        copy._name = this._name;
        return copy;
    }

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
        return from == other.from && size == other.size && Objects.equals(filter, other.filter) && Objects.equals(_name, other._name) && doEquals(other);
    }

    protected abstract boolean doEquals(RB other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), from, size, filter, _name, doHashCode());
    }

    protected abstract int doHashCode();
}
