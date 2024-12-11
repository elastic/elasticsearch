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
import org.elasticsearch.xpack.esql.parser.ParserUtils;
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
import java.util.stream.Collectors;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE_OBJECT_ARRAY;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.WILDCARD;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.isValidParamName;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.IDENTIFIER;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.PATTERN;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.VALUE;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.paramClassifications;

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
    private static final ParseField INCLUDE_CCS_METADATA_FIELD = new ParseField("include_ccs_metadata");
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
        parser.declareBoolean(EsqlQueryRequest::includeCCSMetadata, INCLUDE_CCS_METADATA_FIELD);
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
            Object paramValue = null;
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
                        ParserUtils.ParamClassification classification = null;
                        paramValue = null;
                        String paramName = entry.getKey();
                        checkParamNameValidity(paramName, errors, loc);

                        if (EsqlCapabilities.Cap.NAMED_PARAMETER_FOR_FIELD_AND_FUNCTION_NAMES_SIMPLIFIED_SYNTAX.isEnabled()
                            && entry.getValue() instanceof Map<?, ?> value) {// parameter specified as a key:value pair
                            checkParamValueSize(paramName, value, loc, errors);
                            for (Object keyName : value.keySet()) {
                                classification = getParamClassification(keyName.toString(), errors, loc);
                                if (classification != null) {
                                    paramValue = value.get(keyName);
                                    checkParamValueValidity(classification, paramValue, loc, errors);
                                }
                            }
                        } else {// parameter specifies as a value only
                            paramValue = entry.getValue();
                            classification = VALUE;
                        }
                        type = DataType.fromJava(paramValue);
                        if (type == null) {
                            errors.add(new XContentParseException(loc, entry + " is not supported as a parameter"));
                        }
                        currentParam = new QueryParam(
                            paramName,
                            paramValue,
                            (classification == VALUE) ? type : DataType.NULL,
                            classification
                        );
                        namedParams.add(currentParam);
                    }
                } else {
                    paramValue = null;
                    if (token == XContentParser.Token.VALUE_STRING) {
                        paramValue = p.text();
                        type = DataType.KEYWORD;
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        XContentParser.NumberType numberType = p.numberType();
                        if (numberType == XContentParser.NumberType.INT) {
                            paramValue = p.intValue();
                            type = DataType.INTEGER;
                        } else if (numberType == XContentParser.NumberType.LONG) {
                            paramValue = p.longValue();
                            type = DataType.LONG;
                        } else if (numberType == XContentParser.NumberType.DOUBLE) {
                            paramValue = p.doubleValue();
                            type = DataType.DOUBLE;
                        }
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        paramValue = p.booleanValue();
                        type = DataType.BOOLEAN;
                    } else if (token == XContentParser.Token.VALUE_NULL) {
                        type = DataType.NULL;
                    } else {
                        errors.add(new XContentParseException(loc, token + " is not supported as a parameter"));
                    }
                    currentParam = new QueryParam(null, paramValue, type, VALUE);
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
        if (errors.isEmpty() == false) {
            throw new XContentParseException(
                "Failed to parse params: "
                    + String.join("; ", errors.stream().map(XContentParseException::getMessage).toArray(String[]::new))
            );
        }
        return new QueryParams(namedParams.isEmpty() ? unNamedParams : namedParams);
    }

    private static void checkParamNameValidity(String name, List<XContentParseException> errors, XContentLocation loc) {
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
    }

    private static void checkParamValueSize(
        String paramName,
        Map<?, ?> paramValue,
        XContentLocation loc,
        List<XContentParseException> errors
    ) {
        if (paramValue.size() == 1) {
            return;
        }
        String errorMessage;
        if (paramValue.isEmpty()) {
            errorMessage = " has no valid param attribute";
        } else {
            errorMessage = " has multiple param attributes ["
                + paramValue.keySet().stream().map(Object::toString).collect(Collectors.joining(", "))
                + "]";
        }
        errors.add(
            new XContentParseException(
                loc,
                "["
                    + paramName
                    + "]"
                    + errorMessage
                    + ", only one of "
                    + Arrays.stream(ParserUtils.ParamClassification.values())
                        .map(ParserUtils.ParamClassification::name)
                        .collect(Collectors.joining(", "))
                    + " can be defined in a param"
            )
        );
    }

    private static ParserUtils.ParamClassification getParamClassification(
        String paramKeyName,
        List<XContentParseException> errors,
        XContentLocation loc
    ) {
        ParserUtils.ParamClassification paramType = paramClassifications.get(paramKeyName.toUpperCase(Locale.ROOT));
        if (paramType == null) {
            errors.add(
                new XContentParseException(
                    loc,
                    "["
                        + paramKeyName
                        + "] is not a valid param attribute, a valid attribute is any of "
                        + Arrays.stream(ParserUtils.ParamClassification.values())
                            .map(ParserUtils.ParamClassification::name)
                            .collect(Collectors.joining(", "))
                )
            );
        }
        return paramType;
    }

    private static void checkParamValueValidity(
        ParserUtils.ParamClassification classification,
        Object value,
        XContentLocation loc,
        List<XContentParseException> errors
    ) {
        // If a param is an "identifier" or a "pattern", validate it is a string.
        // If a param is a "pattern", validate it contains *.
        if (classification == IDENTIFIER || classification == PATTERN) {
            if (DataType.fromJava(value) != KEYWORD
                || (classification == PATTERN && value != null && value.toString().contains(WILDCARD) == false)) {
                errors.add(
                    new XContentParseException(
                        loc,
                        format(
                            null,
                            "[{}] is not a valid value for {} parameter, a valid value for {} parameter is a string{}",
                            value,
                            classification.name(),
                            classification.name(),
                            classification == PATTERN ? " and contains *" : ""
                        )
                    )
                );
            }
        }
    }
}
