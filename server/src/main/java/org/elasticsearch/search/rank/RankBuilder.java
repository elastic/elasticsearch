/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RankBuilder implements Writeable, ToXContent {

    private static final ConstructingObjectParser<RankBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "rank",
        args -> new RankBuilder().toRankContext((RRFBuilder) args[0])
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> RRFBuilder.fromXContent(p), RRFBuilder.RANK_NAME);
    }

    public static RankBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (toRankContext != null) {
            builder.field(toRankContext.getRankName().getPreferredName(), toRankContext);
        }
        builder.endObject();

        return builder;
    }

    private ToRankContext toRankContext;

    public RankBuilder() {}

    public RankBuilder(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            String rankName = in.readString();
            if (RRFBuilder.RANK_NAME.getPreferredName().equals(rankName)) {
                toRankContext = in.readOptionalWriteable(RRFBuilder::new);
            } else {
                throw new IllegalStateException("unknown rank name [" + rankName + "]");
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (toRankContext == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(toRankContext.getRankName().getPreferredName());
            out.writeWriteable(toRankContext);
        }
    }

    public RankBuilder toRankContext(ToRankContext toRankContext) {
        this.toRankContext = toRankContext;
        return this;
    }

    public ToRankContext toRankContext() {
        return toRankContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankBuilder that = (RankBuilder) o;
        return Objects.equals(toRankContext, that.toRankContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(toRankContext);
    }

    @Override
    public String toString() {
        return toString(EMPTY_PARAMS);
    }

    public String toString(Params params) {
        try {
            return XContentHelper.toXContent(this, XContentType.JSON, params, true).utf8ToString();
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }
}
