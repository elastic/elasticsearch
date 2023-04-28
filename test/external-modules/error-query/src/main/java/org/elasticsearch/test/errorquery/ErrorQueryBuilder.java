/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.errorquery;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A test query that can simulate errors and warnings when executing a shard request.
 */
public class ErrorQueryBuilder extends AbstractQueryBuilder<ErrorQueryBuilder> {
    public static final String NAME = "error_query";

    private List<IndexError> indices;

    public ErrorQueryBuilder(List<IndexError> indices) {
        this.indices = Objects.requireNonNull(indices);
    }

    public ErrorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readList(IndexError::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(indices);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        // Disable the request cache
        context.nowInMillis();

        IndexError error = null;
        for (IndexError index : indices) {
            if (context.indexMatches(index.getIndexName())) {
                error = index;
                break;
            }
        }
        if (error == null) {
            return new MatchAllDocsQuery();
        }
        if (error.getShardIds() != null) {
            boolean match = false;
            for (int shardId : error.getShardIds()) {
                if (context.getShardId() == shardId) {
                    match = true;
                    break;
                }
            }
            if (match == false) {
                return new MatchAllDocsQuery();
            }
        }
        final String header = "[" + context.index().getName() + "][" + context.getShardId() + "]";
        if (error.getErrorType() == IndexError.ERROR_TYPE.WARNING) {
            HeaderWarning.addWarning(header + " " + error.getMessage());
            return new MatchAllDocsQuery();
        } else {
            throw new RuntimeException(header + " " + error.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ErrorQueryBuilder, String> PARSER = new ConstructingObjectParser<>(NAME, false, (args, name) -> {
        ErrorQueryBuilder q = new ErrorQueryBuilder((List<IndexError>) args[0]);
        final float boost = args[1] == null ? DEFAULT_BOOST : (Float) args[1];
        final String queryName = (String) args[2];
        q.boost = boost;
        q.queryName = queryName;
        return q;
    });

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> IndexError.PARSER.parse(p, c), new ParseField("indices"));
        PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), BOOST_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("indices");
        for (IndexError indexError : indices) {
            builder.startObject();
            indexError.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected boolean doEquals(ErrorQueryBuilder other) {
        return Objects.equals(indices, other.indices);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(indices);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
