/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;

public class RRFRankBuilderTests extends AbstractXContentSerializingTestCase<RRFRankBuilder> {

    @Override
    protected RRFRankBuilder createTestInstance() {
        return new RRFRankBuilder(randomIntBetween(0, 100000), randomIntBetween(1, Integer.MAX_VALUE));
    }

    @Override
    protected RRFRankBuilder mutateInstance(RRFRankBuilder instance) throws IOException {
        if (randomBoolean()) {
            return new RRFRankBuilder(instance.windowSize(), instance.rankConstant() == 1 ? 2 : instance.rankConstant() - 1);
        } else {
            return new RRFRankBuilder(instance.windowSize() == 0 ? 1 : instance.windowSize() - 1, instance.rankConstant());
        }
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
        assertEquals(parser.currentName(), RRFRankPlugin.NAME);
        RRFRankBuilder builder = RRFRankBuilder.PARSER.parse(parser, null);
        parser.nextToken();
        Assert.assertEquals(parser.currentToken(), XContentParser.Token.END_OBJECT);
        parser.nextToken();
        Assert.assertNull(parser.currentToken());
        return builder;
    }

    public void testCreateRankContexts() {
        RRFRankBuilder rrfRankBuilder = createTestInstance();

        List<Query> queries = List.of(new TermQuery(new Term("field0", "test0")), new TermQuery(new Term("field1", "test1")));
        RankShardContext rsc = rrfRankBuilder.buildRankShardContext(queries, randomInt());
        assertEquals(queries, rsc.queries());
        assertEquals(rrfRankBuilder.windowSize(), rsc.windowSize());

        assertNotNull(rrfRankBuilder.buildRankCoordinatorContext(randomInt(), randomInt()));
    }
}
