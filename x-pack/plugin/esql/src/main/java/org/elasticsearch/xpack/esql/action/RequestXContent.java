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
import java.util.BitSet;
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

    private enum ParamParsingKeys {
        VALUE,
        IDENTIFIER,
        IDENTIFIERPATTERN
    };

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
            Object paramValue = null;
            DataType type = null;
            QueryParam currentParam = null;
            TempObjects param;
            HashMap<String, Object> tempMap = new HashMap<>();

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
                        ParserUtils.ParamClassification classification;
                        String paramName = entry.getKey();
                        checkParamNameValidity(paramName, errors, loc);

                        if (entry.getValue() instanceof HashMap<?, ?> values) {// parameter specified as key:value pairs
                            HashMap<ParamParsingKeys, Object> paramElements = new HashMap<>(3);

                            for (Object keyName : values.keySet()) {
                                ParamParsingKeys paramType = checkParamValueKeysValidity(keyName.toString(), errors, loc);
                                if (paramType != null) {
                                    paramElements.put(paramType, values.get(keyName));
                                }
                            }
                            paramValue = paramElements.get(ParamParsingKeys.VALUE);
                            if (paramValue == null && values.size() > 1) { // require non-null value for identifier and pattern
                                errors.add(new XContentParseException(loc, "[" + entry + "] does not have a value provided"));
                            }

                            classification = getClassificationForParam(paramName, paramElements, loc, errors);
                        } else {// parameter specifies as a value only
                            paramValue = entry.getValue();
                            classification = ParserUtils.ParamClassification.CONSTANT;
                        }
                        type = DataType.fromJava(paramValue);
                        if (type == null) {
                            errors.add(new XContentParseException(loc, entry + " is not supported as a parameter"));
                        }
                        currentParam = new QueryParam(
                            paramName,
                            paramValue,
                            (classification == ParserUtils.ParamClassification.CONSTANT) ? type : DataType.NULL,
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
                    currentParam = new QueryParam(null, paramValue, type, ParserUtils.ParamClassification.CONSTANT);
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

    private static ParamParsingKeys checkParamValueKeysValidity(
        String paramKeyName,
        List<XContentParseException> errors,
        XContentLocation loc
    ) {
        ParamParsingKeys paramType = null;
        try {
            paramType = ParamParsingKeys.valueOf(paramKeyName.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException iae) {
            errors.add(
                new XContentParseException(
                    loc,
                    "["
                        + paramKeyName
                        + "] is not a valid param attribute, "
                        + "a valid attribute is any of "
                        + Arrays.stream(ParamParsingKeys.values())
                            .map(p -> p.toString().toLowerCase(Locale.ROOT))
                            .collect(Collectors.joining(", "))
                )
            );
        }
        return paramType;
    }

    private static ParserUtils.ParamClassification getClassificationForParam(
        String paramName,
        HashMap<ParamParsingKeys, Object> paramElements,
        XContentLocation loc,
        List<XContentParseException> errors
    ) {
        ParserUtils.ParamClassification classification = ParserUtils.ParamClassification.CONSTANT;
        Object value = paramElements.get(ParamParsingKeys.VALUE);
        // paramAttributes(0) = true if this is set as an identifier, paramAttributes(1) = true if this is set as an identifier pattern
        BitSet identifierOrPattern = new BitSet(2);
        var id = paramElements.get(ParamParsingKeys.IDENTIFIER);
        if (isValidateIdentifierOrPatternField(id, ParserUtils.ParamClassification.IDENTIFIER, loc, errors)) {
            identifierOrPattern.set(0);
            classification = ParserUtils.ParamClassification.IDENTIFIER;
        }
        var pattern = paramElements.get(ParamParsingKeys.IDENTIFIERPATTERN);
        if (isValidateIdentifierOrPatternField(pattern, ParserUtils.ParamClassification.PATTERN, loc, errors)) {
            identifierOrPattern.set(1);
            classification = ParserUtils.ParamClassification.PATTERN;
        }
        // a param cannot be both identifier and pattern
        if (identifierOrPattern.cardinality() > 1) {
            errors.add(
                new XContentParseException(
                    loc,
                    format(
                        null,
                        "[{} = {value={}, {}=true, {}=true}] can be either an {} or an {}, but cannot be both",
                        paramName,
                        value,
                        ParamParsingKeys.IDENTIFIER.toString().toLowerCase(Locale.ROOT),
                        ParamParsingKeys.IDENTIFIERPATTERN.toString().toLowerCase(Locale.ROOT),
                        ParamParsingKeys.IDENTIFIER.toString().toLowerCase(Locale.ROOT),
                        ParamParsingKeys.IDENTIFIERPATTERN.toString().toLowerCase(Locale.ROOT)
                    )
                )
            );
        } else {
            classification = identifierOrPattern.get(0)
                ? ParserUtils.ParamClassification.IDENTIFIER
                : ParserUtils.ParamClassification.PATTERN;
        }
        // If a param is an "identifier" or "identifierpattern", validate it is a string.
        // If a param is an "identifierpattern", validate it contains *.
        if (classification == ParserUtils.ParamClassification.IDENTIFIER || classification == ParserUtils.ParamClassification.PATTERN) {
            if (DataType.fromJava(value) != KEYWORD
                || (classification == ParserUtils.ParamClassification.PATTERN
                    && value != null
                    && value.toString().contains(WILDCARD) == false)) {
                errors.add(
                    new XContentParseException(
                        loc,
                        format(
                            null,
                            "[{}] is not a valid value for {} parameter, a valid value for {} parameter is a string{}",
                            value,
                            classification.toString().toLowerCase(Locale.ROOT),
                            classification.toString().toLowerCase(Locale.ROOT),
                            classification == ParserUtils.ParamClassification.PATTERN ? " and contains *" : ""
                        )
                    )
                );
            }
        }
        return classification;
    }

    private static boolean isValidateIdentifierOrPatternField(
        Object value,
        ParserUtils.ParamClassification expected,
        XContentLocation loc,
        List<XContentParseException> errors
    ) {
        if (value != null) {
            if ((value instanceof Boolean) == false) {
                // "identifier" and "identifierpattern" can only be a boolean value, true or false.
                errors.add(
                    new XContentParseException(
                        loc,
                        format(
                            null,
                            "[{}] is not a valid value for an identifier{} field, a valid value is either true or false",
                            value,
                            expected == ParserUtils.ParamClassification.IDENTIFIER ? "" : " pattern"
                        )
                    )
                );
            } else {
                return (Boolean) value;
            }
        }
        return false;
    }
}
