/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.rank.RankBuilderTests;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Assert;

import java.io.IOException;

public class RRFRankBuilderTests extends RankBuilderTests<RRFRankBuilder> {

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
        Assert.assertEquals(parser.currentToken(), XContentParser.Token.START_OBJECT);
        parser.nextToken();
        assertEquals(parser.currentToken(), XContentParser.Token.FIELD_NAME);
        assertEquals(parser.currentName(), RankRRFPlugin.NAME);
        RRFRankBuilder builder = RRFRankBuilder.PARSER.parse(parser, new RRFRankBuilder(), null);
        parser.nextToken();
        Assert.assertEquals(parser.currentToken(), XContentParser.Token.END_OBJECT);
        parser.nextToken();
        Assert.assertNull(parser.currentToken());
        return builder;
    }
}
