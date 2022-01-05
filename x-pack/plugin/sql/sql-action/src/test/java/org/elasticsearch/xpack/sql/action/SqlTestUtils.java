/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.proto.content.ContentFactory;
import org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType.CBOR;
import static org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType.JSON;

public final class SqlTestUtils {

    private SqlTestUtils() {

    }

    /**
     * Returns a random QueryBuilder or null
     */
    public static QueryBuilder randomFilterOrNull(Random random) {
        final QueryBuilder randomFilter;
        if (random.nextBoolean()) {
            randomFilter = randomFilter(random);
        } else {
            randomFilter = null;
        }
        return randomFilter;
    }

    /**
     * Returns a random QueryBuilder
     */
    public static QueryBuilder randomFilter(Random random) {
        return new RangeQueryBuilder(RandomStrings.randomAsciiLettersOfLength(random, 10)).gt(random.nextInt());
    }

    static void assumeXContentJsonOrCbor(XContentType type) {
        LuceneTestCase.assumeTrue(
            "only JSON/CBOR xContent supported; ignoring " + type.name(),
            type == XContentType.JSON || type == XContentType.CBOR
        );
    }

    static <T> T fromXContentParser(XContentParser parser, CheckedFunction<JsonParser, T, IOException> objectParser) throws IOException {
        // load object as a map
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // save it back to a stream
        XContentGenerator generator = parser.contentType().xContent().createGenerator(out);
        generator.copyCurrentStructure(parser);
        generator.close();
        // System.out.println(out.toString(StandardCharsets.UTF_8));
        ContentType type = parser.contentType() == XContentType.JSON ? JSON : CBOR;
        return objectParser.apply(ContentFactory.parser(type, new ByteArrayInputStream(out.toByteArray())));
    }
}
