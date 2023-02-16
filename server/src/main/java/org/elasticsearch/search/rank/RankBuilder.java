/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

public class RankBuilder implements Writeable, ToXContent {

    public static RankBuilder fromXContent(XContentParser parser) throws IOException {
        RankBuilder rankBuilder;
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + parser.currentToken() + "]");
        }
        parser.nextToken();
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + parser.currentToken() + "]");
        }
        String fieldName = parser.currentName();
        if (RRFRankContextBuilder.RANK_NAME.getPreferredName().equals(fieldName)) {
            rankBuilder = new RankBuilder().toRankContext(RRFRankContextBuilder.fromXContent(parser));
        } else {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + parser.currentToken() + "]");
        }
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + parser.currentToken() + "]");
        }
        parser.nextToken();
        return rankBuilder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (rankContextBuilder != null) {
            builder.field(rankContextBuilder.name().getPreferredName(), rankContextBuilder);
        }
        builder.endObject();

        return builder;
    }

    private RankContextBuilder rankContextBuilder;

    public RankBuilder() {}

    public RankBuilder(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            String rankName = in.readString();
            if (RRFRankContextBuilder.RANK_NAME.getPreferredName().equals(rankName)) {
                rankContextBuilder = in.readOptionalWriteable(RRFRankContextBuilder::new);
            } else {
                throw new IllegalStateException("unknown rank name [" + rankName + "]");
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (rankContextBuilder == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(rankContextBuilder.name().getPreferredName());
            out.writeWriteable(rankContextBuilder);
        }
    }

    public RankBuilder toRankContext(RankContextBuilder rankContextBuilder) {
        this.rankContextBuilder = rankContextBuilder;
        return this;
    }

    public RankContextBuilder toRankContext() {
        return rankContextBuilder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankBuilder that = (RankBuilder) o;
        return Objects.equals(rankContextBuilder, that.rankContextBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rankContextBuilder);
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
