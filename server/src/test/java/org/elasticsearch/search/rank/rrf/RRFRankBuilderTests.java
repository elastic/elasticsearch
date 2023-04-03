/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.rank.RankBuilderTests;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class RRFRankBuilderTests extends RankBuilderTests<RRFRankBuilder> {

    public static RRFRankBuilder randomRankContextBuilder() {
        RRFRankBuilder builder = new RRFRankBuilder();
        builder.rankConstant(randomIntBetween(1, Integer.MAX_VALUE));
        RankBuilderTests.randomRankContextBuilder(builder);
        return builder;
    }

    @Override
    protected RRFRankBuilder doCreateTestInstance() {
        RRFRankBuilder builder = new RRFRankBuilder();
        builder.rankConstant(randomIntBetween(1, Integer.MAX_VALUE));
        return builder;
    }

    @Override
    protected RRFRankBuilder doMutateInstance(RRFRankBuilder instance) throws IOException {
        RRFRankBuilder builder = new RRFRankBuilder();
        if (randomBoolean()) {
            builder.rankConstant(instance.rankConstant() == 1 ? 2 : instance.rankConstant() - 1);
        }
        return builder;
    }

    @Override
    protected Writeable.Reader<RRFRankBuilder> instanceReader() {
        return RRFRankBuilder::new;
    }

    @Override
    protected RRFRankBuilder doParseInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.START_OBJECT);
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.FIELD_NAME);
        assertEquals(parser.currentName(), RRFRankBuilder.NAME);
        RRFRankBuilder builder = RRFRankBuilder.fromXContent(parser);
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.END_OBJECT);
        parser.nextToken();
        assertNull(parser.currentToken());
        return builder;
    }
}
