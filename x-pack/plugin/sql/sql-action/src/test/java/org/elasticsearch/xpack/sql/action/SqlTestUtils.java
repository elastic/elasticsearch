/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Payloads;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.proto.content.ContentFactory;
import org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType;
import org.elasticsearch.xpack.sql.proto.content.GeneratorUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;

import static org.elasticsearch.xpack.sql.proto.CoreProtocol.FILTER_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.RUNTIME_MAPPINGS_NAME;
import static org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType.CBOR;
import static org.elasticsearch.xpack.sql.proto.content.ContentFactory.ContentType.JSON;
import static org.junit.Assert.assertEquals;

public final class SqlTestUtils {

    private SqlTestUtils() {}

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

    static <T> T fromXContentParser(XContentParser parser, CheckedFunction<JsonParser, T, IOException> objectParser) {
        // load object as a map
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            // save it back to a stream
            XContentGenerator generator = parser.contentType().xContent().createGenerator(out);
            generator.copyCurrentStructure(parser);
            generator.close();
            // System.out.println(out.toString(StandardCharsets.UTF_8));
            ContentType type = parser.contentType() == XContentType.JSON ? JSON : CBOR;
            return objectParser.apply(ContentFactory.parser(type, new ByteArrayInputStream(out.toByteArray())));
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Bridge proto classes to XContent by using the sql-proto serialization then loading the stream into XContentBuilder
     */
    static XContentBuilder toXContentBuilder(XContentBuilder builder, CheckedConsumer<JsonGenerator, IOException> objectGenerator) {
        ContentType type = builder.contentType() == XContentType.JSON ? JSON : CBOR;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            JsonGenerator generator = ContentFactory.generator(type, out);
            objectGenerator.accept(generator);
            generator.close();
            // System.out.println(out.toString(StandardCharsets.UTF_8));
            XContentParser parser = builder.contentType()
                .xContent()
                .createParser(XContentParserConfiguration.EMPTY, new ByteArrayInputStream(out.toByteArray()));
            builder.copyCurrentStructure(parser);
            builder.flush();
            ByteArrayOutputStream stream = (ByteArrayOutputStream) builder.getOutputStream();
            assertEquals("serialized objects differ", out.toString(StandardCharsets.UTF_8), stream.toString(StandardCharsets.UTF_8));
            return builder;
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Specialized method of the above that extends the SqlQueryRequest serialization from sql-proto with sql-action.
     * The extra fields are serialized through XContent however the serialized form is then copied over to the same stream.
     */
    static XContentBuilder toXContentBuilder(
        XContentBuilder builder,
        AbstractSqlQueryRequest actionRequest,
        org.elasticsearch.xpack.sql.proto.SqlQueryRequest protoRequest
    ) {
        return toXContentBuilder(
            builder,
            g -> Payloads.generate(
                g,
                protoRequest,
                (generator, p) -> generateSqlParamWithMode(generator, p, actionRequest.mode()),
                generator -> generateFilterAndRuntimeMappings(generator, actionRequest, builder.contentType())
            )
        );
    }

    // custom sql param serialization
    private static void generateSqlParamWithMode(JsonGenerator generator, SqlTypedParamValue param, Mode mode) throws IOException {
        if (Mode.isDriver(mode)) {
            // default (explicit) serialization
            Payloads.generate(generator, param);
        } else {
            // the type is implicit, specify only the value
            GeneratorUtils.unknownValue(generator, param.value);
        }
    }

    private static void generateFilterAndRuntimeMappings(
        JsonGenerator generator,
        AbstractSqlQueryRequest request,
        XContentType xContentType
    ) throws IOException {
        if (request.filter() != null) {
            generator.writeFieldName(FILTER_NAME);
            SqlTestUtils.copyFilterAsValue(xContentType, request.filter(), generator);
        }
        if (request.runtimeMappings() != null) {
            generator.writeFieldName(RUNTIME_MAPPINGS_NAME);
            GeneratorUtils.unknownValue(generator, request.runtimeMappings());
        }
    }

    static void copyFilterAsValue(XContentType xContentType, QueryBuilder filter, JsonGenerator generator) throws IOException {
        BytesReference bytesReference = XContentHelper.toXContent(filter, xContentType, false);
        Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(bytesReference, true, xContentType);
        GeneratorUtils.unknownValue(generator, tuple.v2());
    }

    static <T> T clone(Writeable writeable, Writeable.Reader<T> reader, NamedWriteableRegistry namedWriteableRegistry) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeable.writeTo(out);
            return reader.read(new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry));
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
