/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;

import org.elasticsearch.xpack.sql.proto.content.ConstructingObjectParser;
import org.elasticsearch.xpack.sql.proto.content.GeneratorUtils;
import org.elasticsearch.xpack.sql.proto.content.ParseException;
import org.elasticsearch.xpack.sql.proto.content.ParserUtils;
import org.elasticsearch.xpack.sql.proto.core.CheckedBiConsumer;
import org.elasticsearch.xpack.sql.proto.core.CheckedConsumer;
import org.elasticsearch.xpack.sql.proto.core.TimeValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.sql.proto.CoreProtocol.BINARY_FORMAT_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.CATALOG_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.CLIENT_ID_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.COLUMNAR_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.COLUMNS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.CURSOR_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.FETCH_SIZE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.FIELD_MULTI_VALUE_LENIENCY_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.ID_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.INDEX_INCLUDE_FROZEN_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.IS_PARTIAL_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.IS_RUNNING_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.KEEP_ALIVE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.KEEP_ON_COMPLETION_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.MODE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.PAGE_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.PARAMS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.QUERY_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.REQUEST_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.ROWS_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.TIME_ZONE_NAME;
import static org.elasticsearch.xpack.sql.proto.CoreProtocol.WAIT_FOR_COMPLETION_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.proto.content.AbstractObjectParser.ValueType.OBJECT_ARRAY;
import static org.elasticsearch.xpack.sql.proto.content.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.sql.proto.content.ConstructingObjectParser.optionalConstructorArg;

public final class Payloads {

    private static final ConstructingObjectParser<MainResponse, Void> MAIN_RESPONSE_PARSER = new ConstructingObjectParser<>(
        "sql/response/main_response",
        true,
        objects -> new MainResponse((String) objects[0], (String) objects[1], (String) objects[2], (String) objects[3])
    );

    private static final ConstructingObjectParser<ColumnInfo, Void> COLUMN_INFO_PARSER = new ConstructingObjectParser<>(
        "sql/response/column_info",
        true,
        objects -> new ColumnInfo(
            objects[0] == null ? "" : (String) objects[0],
            (String) objects[1],
            (String) objects[2],
            (Integer) objects[3]
        )
    );

    private static final ConstructingObjectParser<ColumnInfo, Void> ROWS_PARSER = new ConstructingObjectParser<>(
        "sql/response/row",
        true,
        objects -> new ColumnInfo(
            objects[0] == null ? "" : (String) objects[0],
            (String) objects[1],
            (String) objects[2],
            (Integer) objects[3]
        )
    );

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SqlQueryResponse, Void> QUERY_RESPONSE_PARSER = new ConstructingObjectParser<>(
        "sql/response/query",
        true,
        objects -> new SqlQueryResponse(
            objects[0] == null ? "" : (String) objects[0],
            (List<ColumnInfo>) objects[1],
            (List<List<Object>>) objects[2],
            (String) objects[3],
            objects[4] != null && (boolean) objects[4],
            objects[5] != null && (boolean) objects[5]
        )
    );

    public static final ConstructingObjectParser<SqlClearCursorResponse, Void> CLEAR_CURSOR_PARSER = new ConstructingObjectParser<>(
        "sql/response/clear_cursor",
        true,
        objects -> new SqlClearCursorResponse(objects[0] != null && (boolean) objects[0])
    );

    static {
        MAIN_RESPONSE_PARSER.declareString(constructorArg(), "name");
        MAIN_RESPONSE_PARSER.declareObject(constructorArg(), (p, c) -> ParserUtils.map(p).get("number"), "version");
        MAIN_RESPONSE_PARSER.declareString(constructorArg(), "cluster_name");
        MAIN_RESPONSE_PARSER.declareString(constructorArg(), "cluster_uuid");
        MAIN_RESPONSE_PARSER.declareString((response, value) -> {}, "tagline");

        COLUMN_INFO_PARSER.declareString(optionalConstructorArg(), "table");
        COLUMN_INFO_PARSER.declareString(constructorArg(), "name");
        COLUMN_INFO_PARSER.declareString(constructorArg(), "type");
        COLUMN_INFO_PARSER.declareInt(optionalConstructorArg(), "display_size");

        QUERY_RESPONSE_PARSER.declareString(optionalConstructorArg(), CURSOR_NAME);
        QUERY_RESPONSE_PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> parseColumnInfo(p), COLUMNS_NAME);
        QUERY_RESPONSE_PARSER.declareField(constructorArg(), (p, c) -> ParserUtils.list(p), ROWS_NAME, OBJECT_ARRAY);
        QUERY_RESPONSE_PARSER.declareString(optionalConstructorArg(), ID_NAME);
        QUERY_RESPONSE_PARSER.declareBoolean(optionalConstructorArg(), IS_PARTIAL_NAME);
        QUERY_RESPONSE_PARSER.declareBoolean(optionalConstructorArg(), IS_RUNNING_NAME);

        CLEAR_CURSOR_PARSER.declareBoolean(optionalConstructorArg(), "succeeded");
    }

    private Payloads() {}

    public static MainResponse parseMainResponse(JsonParser parser) throws ParseException {
        return MAIN_RESPONSE_PARSER.apply(parser, null);
    }

    public static SqlQueryResponse parseQueryResponse(JsonParser parser) throws ParseException {
        return QUERY_RESPONSE_PARSER.apply(parser, null);
    }

    public static ColumnInfo parseColumnInfo(JsonParser parser) throws ParseException {
        return COLUMN_INFO_PARSER.apply(parser, null);
    }

    public static SqlClearCursorResponse parseClearCursorResponse(JsonParser parser) throws ParseException {
        return CLEAR_CURSOR_PARSER.apply(parser, null);
    }

    public static void generate(JsonGenerator generator, AbstractSqlRequest sqlRequest) throws IOException {
        if (sqlRequest instanceof SqlClearCursorRequest) {
            generate(generator, (SqlClearCursorRequest) sqlRequest);
        } else if (sqlRequest instanceof SqlQueryRequest) {
            generate(generator, (SqlQueryRequest) sqlRequest);
        }
    }

    public static void generate(JsonGenerator generator, ColumnInfo info) throws IOException {
        generator.writeStartObject();

        String table = info.table();
        if (table != null && table.isEmpty() == false) {
            generator.writeStringField("table", table);
        }
        generator.writeStringField("name", info.name());
        generator.writeStringField("type", info.esType());
        if (info.displaySize() != null) {
            generator.writeNumberField("display_size", info.displaySize());
        }

        generator.writeEndObject();
    }

    public static void generate(JsonGenerator generator, SqlClearCursorRequest request) throws IOException {
        generator.writeStartObject();

        generator.writeStringField("cursor", request.getCursor());
        generator.writeStringField("mode", request.mode().toString());
        writeIfValid(generator, "client_id", request.clientId());
        writeIfValidAsString(generator, "version", request.version());

        generator.writeEndObject();
    }

    public static void generate(JsonGenerator generator, SqlTypedParamValue param) throws IOException {
        generator.writeStartObject();

        generator.writeStringField("type", param.type);
        generator.writeFieldName("value");
        GeneratorUtils.unknownValue(generator, param.value);

        generator.writeEndObject();
    }

    public static void generate(JsonGenerator generator, SqlQueryRequest request) throws IOException {
        generate(generator, request, Payloads::generate, null);
    }

    // Allow extra serialization to happen for SqlQueryRequest to allow testing serialization for filter and runtime mappings in sql-action
    public static void generate(
        JsonGenerator generator,
        SqlQueryRequest request,
        CheckedBiConsumer<JsonGenerator, SqlTypedParamValue, IOException> sqlParamGenerator,
        CheckedConsumer<JsonGenerator, IOException> extraFields
    ) throws IOException {
        generator.writeStartObject();

        writeIfValid(generator, QUERY_NAME, request.query());
        generator.writeStringField(MODE_NAME, request.mode().toString());
        writeIfValid(generator, CLIENT_ID_NAME, request.clientId());
        writeIfValidAsString(generator, "version", request.version());

        List<SqlTypedParamValue> params = request.params();
        if (params != null && params.isEmpty() == false) {
            generator.writeArrayFieldStart(PARAMS_NAME);
            for (SqlTypedParamValue param : params) {
                sqlParamGenerator.accept(generator, param);
            }
            generator.writeEndArray();
        }
        writeIfValidAsString(generator, TIME_ZONE_NAME, request.zoneId(), ZoneId::getId);
        writeIfValid(generator, CATALOG_NAME, request.catalog());

        if (request.fetchSize() != CoreProtocol.FETCH_SIZE) {
            generator.writeNumberField(FETCH_SIZE_NAME, request.fetchSize());
        }
        if (request.requestTimeout() != CoreProtocol.REQUEST_TIMEOUT) {
            generator.writeStringField(REQUEST_TIMEOUT_NAME, request.requestTimeout().getStringRep());
        }
        if (request.pageTimeout() != CoreProtocol.PAGE_TIMEOUT) {
            generator.writeStringField(PAGE_TIMEOUT_NAME, request.pageTimeout().getStringRep());
        }
        if (request.columnar() != null) {
            generator.writeBooleanField(COLUMNAR_NAME, request.columnar());
        }
        if (request.fieldMultiValueLeniency()) {
            generator.writeBooleanField(FIELD_MULTI_VALUE_LENIENCY_NAME, request.fieldMultiValueLeniency());
        }
        if (request.indexIncludeFrozen()) {
            generator.writeBooleanField(INDEX_INCLUDE_FROZEN_NAME, request.indexIncludeFrozen());
        }
        if (request.binaryCommunication() != null) {
            generator.writeBooleanField(BINARY_FORMAT_NAME, request.binaryCommunication());
        }
        writeIfValid(generator, CURSOR_NAME, request.cursor());

        writeIfValidAsString(generator, WAIT_FOR_COMPLETION_TIMEOUT_NAME, request.waitForCompletionTimeout(), TimeValue::getStringRep);

        if (request.keepOnCompletion()) {
            generator.writeBooleanField(KEEP_ON_COMPLETION_NAME, request.keepOnCompletion());
        }
        writeIfValidAsString(generator, KEEP_ALIVE_NAME, request.keepAlive(), TimeValue::getStringRep);

        if (extraFields != null) {
            extraFields.accept(generator);
        }
        generator.writeEndObject();
    }

    private static void writeIfValid(JsonGenerator generator, String name, String value) throws IOException {
        if (value != null) {
            generator.writeStringField(name, value);
        }
    }

    private static void writeIfValidAsString(JsonGenerator generator, String name, Object value) throws IOException {
        writeIfValidAsString(generator, name, value, Object::toString);
    }

    private static <T> void writeIfValidAsString(JsonGenerator generator, String name, T value, Function<T, String> toString)
        throws IOException {
        if (value != null) {
            generator.writeStringField(name, toString.apply(value));
        }
    }
}
