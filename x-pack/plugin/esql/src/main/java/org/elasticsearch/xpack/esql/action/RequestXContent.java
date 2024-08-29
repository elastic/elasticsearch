/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE_OBJECT_ARRAY;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.isValidParamName;

/** Static methods for parsing xcontent requests to transport requests. */
final class RequestXContent {

    private static class TempObjects {
        Map<String, Object> fields = new HashMap<>();

        TempObjects() {}

        void addField(String key, Object value) {
            fields.put(key, value);
        }

        String fields() {
            StringBuffer s = new StringBuffer();
            for (Map.Entry<?, ?> entry : fields.entrySet()) {
                if (s.length() > 0) {
                    s.append(", ");
                }
                s.append("{").append(entry.getKey()).append(":").append(entry.getValue()).append("}");
            }
            return s.toString();
        }
    }

    private static final ObjectParser<TempObjects, Void> PARAM_PARSER = new ObjectParser<>(
        "params",
        TempObjects::addField,
        TempObjects::new
    );

    static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField COLUMNAR_FIELD = new ParseField("columnar");
    private static final ParseField FILTER_FIELD = new ParseField("filter");
    static final ParseField PRAGMA_FIELD = new ParseField("pragma");
    private static final ParseField PARAMS_FIELD = new ParseField("params");
    private static final ParseField LOCALE_FIELD = new ParseField("locale");
    private static final ParseField PROFILE_FIELD = new ParseField("profile");
    private static final ParseField ACCEPT_PRAGMA_RISKS = new ParseField("accept_pragma_risks");
    static final ParseField TABLES_FIELD = new ParseField("tables");

    static final ParseField WAIT_FOR_COMPLETION_TIMEOUT = new ParseField("wait_for_completion_timeout");
    static final ParseField KEEP_ALIVE = new ParseField("keep_alive");
    static final ParseField KEEP_ON_COMPLETION = new ParseField("keep_on_completion");

    private static final ObjectParser<EsqlQueryRequest, Void> SYNC_PARSER = objectParserSync(EsqlQueryRequest::syncEsqlQueryRequest);
    private static final ObjectParser<EsqlQueryRequest, Void> ASYNC_PARSER = objectParserAsync(EsqlQueryRequest::asyncEsqlQueryRequest);

    /** Parses a synchronous request. */
    static EsqlQueryRequest parseSync(XContentParser parser) {
        return SYNC_PARSER.apply(parser, null);
    }

    /** Parses an asynchronous request. */
    static EsqlQueryRequest parseAsync(XContentParser parser) {
        return ASYNC_PARSER.apply(parser, null);
    }

    private static void objectParserCommon(ObjectParser<EsqlQueryRequest, ?> parser) {
        parser.declareString(EsqlQueryRequest::query, QUERY_FIELD);
        parser.declareBoolean(EsqlQueryRequest::columnar, COLUMNAR_FIELD);
        parser.declareObject(EsqlQueryRequest::filter, (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p), FILTER_FIELD);
        parser.declareBoolean(EsqlQueryRequest::acceptedPragmaRisks, ACCEPT_PRAGMA_RISKS);
        parser.declareObject(
            EsqlQueryRequest::pragmas,
            (p, c) -> new QueryPragmas(Settings.builder().loadFromMap(p.map()).build()),
            PRAGMA_FIELD
        );
        parser.declareField(EsqlQueryRequest::params, RequestXContent::parseParams, PARAMS_FIELD, VALUE_OBJECT_ARRAY);
        parser.declareString((request, localeTag) -> request.locale(Locale.forLanguageTag(localeTag)), LOCALE_FIELD);
        parser.declareBoolean(EsqlQueryRequest::profile, PROFILE_FIELD);
        parser.declareField((p, r, c) -> new ParseTables(r, p).parseTables(), TABLES_FIELD, ObjectParser.ValueType.OBJECT);
    }

    private static ObjectParser<EsqlQueryRequest, Void> objectParserSync(Supplier<EsqlQueryRequest> supplier) {
        ObjectParser<EsqlQueryRequest, Void> parser = new ObjectParser<>("esql/query", false, supplier);
        objectParserCommon(parser);
        return parser;
    }

    private static ObjectParser<EsqlQueryRequest, Void> objectParserAsync(Supplier<EsqlQueryRequest> supplier) {
        ObjectParser<EsqlQueryRequest, Void> parser = new ObjectParser<>("esql/async_query", false, supplier);
        objectParserCommon(parser);
        parser.declareBoolean(EsqlQueryRequest::keepOnCompletion, KEEP_ON_COMPLETION);
        parser.declareField(
            EsqlQueryRequest::waitForCompletionTimeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), WAIT_FOR_COMPLETION_TIMEOUT.getPreferredName()),
            WAIT_FOR_COMPLETION_TIMEOUT,
            ObjectParser.ValueType.VALUE
        );
        parser.declareField(
            EsqlQueryRequest::keepAlive,
            (p, c) -> TimeValue.parseTimeValue(p.text(), KEEP_ALIVE.getPreferredName()),
            KEEP_ALIVE,
            ObjectParser.ValueType.VALUE
        );
        return parser;
    }

    private static QueryParams parseParams(XContentParser p) throws IOException {
        List<QueryParam> namedParams = new ArrayList<>();
        List<QueryParam> unNamedParams = new ArrayList<>();
        List<XContentParseException> errors = new ArrayList<>();
        XContentParser.Token token = p.currentToken();

        if (token == XContentParser.Token.START_ARRAY) {
            Object value = null;
            DataType type = null;
            QueryParam currentParam = null;
            TempObjects param;

            while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                XContentLocation loc = p.getTokenLocation();
                if (token == XContentParser.Token.START_OBJECT) {
                    param = PARAM_PARSER.apply(p, null);
                    if (param.fields.size() > 1) {
                        errors.add(
                            new XContentParseException(
                                loc,
                                "Cannot parse more than one key:value pair as parameter, found [" + param.fields() + "]"
                            )
                        );
                    }
                    for (Map.Entry<String, Object> entry : param.fields.entrySet()) {
                        String name = entry.getKey();
                        if (isValidParamName(name) == false) {
                            errors.add(
                                new XContentParseException(
                                    loc,
                                    "["
                                        + name
                                        + "] is not a valid parameter name, "
                                        + "a valid parameter name starts with a letter or underscore, "
                                        + "and contains letters, digits and underscores only"
                                )
                            );
                        }
                        type = DataType.fromJava(entry.getValue());
                        if (type == null) {
                            errors.add(new XContentParseException(loc, entry + " is not supported as a parameter"));
                        }
                        currentParam = new QueryParam(name, entry.getValue(), type);
                        namedParams.add(currentParam);
                    }
                } else {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        value = p.text();
                        type = DataType.KEYWORD;
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        XContentParser.NumberType numberType = p.numberType();
                        if (numberType == XContentParser.NumberType.INT) {
                            value = p.intValue();
                            type = DataType.INTEGER;
                        } else if (numberType == XContentParser.NumberType.LONG) {
                            value = p.longValue();
                            type = DataType.LONG;
                        } else if (numberType == XContentParser.NumberType.DOUBLE) {
                            value = p.doubleValue();
                            type = DataType.DOUBLE;
                        }
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        value = p.booleanValue();
                        type = DataType.BOOLEAN;
                    } else if (token == XContentParser.Token.VALUE_NULL) {
                        value = null;
                        type = DataType.NULL;
                    } else {
                        errors.add(new XContentParseException(loc, token + " is not supported as a parameter"));
                    }
                    currentParam = new QueryParam(null, value, type);
                    unNamedParams.add(currentParam);
                }
            }
        }
        // don't allow mixed named and unnamed parameters
        if (namedParams.isEmpty() == false && unNamedParams.isEmpty() == false) {
            errors.add(
                new XContentParseException(
                    "Params cannot contain both named and unnamed parameters; got "
                        + Arrays.toString(namedParams.stream().map(QueryParam::nameValue).toArray())
                        + " and "
                        + Arrays.toString(unNamedParams.stream().map(QueryParam::nameValue).toArray())
                )
            );
        }
        if (errors.size() > 0) {
            throw new XContentParseException(
                "Failed to parse params: " + String.join("; ", errors.stream().map(ex -> ex.getMessage()).toArray(String[]::new))
            );
        }
        return new QueryParams(namedParams.isEmpty() ? unNamedParams : namedParams);
    }
}
