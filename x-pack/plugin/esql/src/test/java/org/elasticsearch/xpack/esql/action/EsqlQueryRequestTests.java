/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.ParserUtils;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plugin.EsqlQueryStatus;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsConstant;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsIdentifier;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.paramAsPattern;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class EsqlQueryRequestTests extends ESTestCase {

    public void testParseFields() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        List<QueryParam> params = randomParameters();
        boolean hasParams = params.isEmpty() == false;
        StringBuilder paramsString = paramsString(params, hasParams);
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
                %s""", query, columnar, timeZone.getId(), locale.toLanguageTag(), filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(timeZone, request.timeZone());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(params.size(), request.params().size());
        for (int i = 0; i < params.size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testNamedParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        String paramsString = """
            ,"params":[ {"n1" : "8.15.0"}, { "n2" : 0.05}, {"n3" : -799810013},
             {"n4" : "127.0.0.1"}, {"n5" : "esql"}, {"n_6" : null}, {"n7_" : false},
             {"_n1" : "8.15.0"}, { "__n2" : 0.05}, {"__3" : -799810013},
             {"__4n" : "127.0.0.1"}, {"_n5" : "esql"}, {"_n6" : null}, {"_n7" : false},
             {"_n8": ["8.15.0", "8.19.0"]}, {"_n9": ["x", "y"]}, {"_n10": [true, false]}, {"_n11": [1.0, 1.1, 1.2]},
             {"_n12": [-799810013, 0, 799810013]}
             ] }""";

        List<QueryParam> params = List.of(
            paramAsConstant("n1", "8.15.0"),
            paramAsConstant("n2", 0.05),
            paramAsConstant("n3", -799810013),
            paramAsConstant("n4", "127.0.0.1"),
            paramAsConstant("n5", "esql"),
            paramAsConstant("n_6", null),
            paramAsConstant("n7_", false),
            paramAsConstant("_n1", "8.15.0"),
            paramAsConstant("__n2", 0.05),
            paramAsConstant("__3", -799810013),
            paramAsConstant("__4n", "127.0.0.1"),
            paramAsConstant("_n5", "esql"),
            paramAsConstant("_n6", null),
            paramAsConstant("_n7", false),
            new QueryParam("_n8", List.of("8.15.0", "8.19.0"), KEYWORD, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n9", List.of("x", "y"), KEYWORD, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n10", List.of(true, false), BOOLEAN, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n11", List.of(1.0, 1.1, 1.2), DOUBLE, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n12", List.of(-799810013, 0, 799810013), DataType.INTEGER, ParserUtils.ParamClassification.VALUE)
            // TODO add mixed null values, or check all elements, and separate into a new method
        );
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
                %s""", query, columnar, timeZone.getId(), locale.toLanguageTag(), filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(timeZone, request.timeZone());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(params.size(), request.params().size());

        for (int i = 0; i < request.params().size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testNamedMultivaluedParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        String paramsString = """
            ,"params":[
             {"_n1": ["8.15.0", "8.19.0"]}, {"_n2": ["x", "y"]}, {"_n3": [true, false]}, {"_n4": [1.0, 1.1, 1.2]},
             {"_n5": [-799810013, 0, 799810013]}
             ] }""";

        List<QueryParam> params = List.of(
            new QueryParam("_n1", List.of("8.15.0", "8.19.0"), KEYWORD, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n2", List.of("x", "y"), KEYWORD, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n3", List.of(true, false), BOOLEAN, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n4", List.of(1.0, 1.1, 1.2), DOUBLE, ParserUtils.ParamClassification.VALUE),
            new QueryParam("_n5", List.of(-799810013, 0, 799810013), DataType.INTEGER, ParserUtils.ParamClassification.VALUE)
        );
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
                %s""", query, columnar, timeZone.getId(), locale.toLanguageTag(), filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(timeZone, request.timeZone());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(params.size(), request.params().size());

        for (int i = 0; i < request.params().size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testNamedParamsForIdentifiersPatterns() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        String paramsString = """
            ,"params":[ {"n1" : {"identifier" : "f1"}},
             {"n2" : {"Identifier" : "f1*"}},
             {"n3" : {"pattern" : "f.1*"}},
             {"n4" : {"Pattern" : "*"}},
             {"n5" : {"Value" : "esql"}},
             {"n_6" : {"identifier" : "null"}},
             {"n7_" : {"value" : "f.1.1"}}] }""";

        List<QueryParam> params = List.of(
            paramAsIdentifier("n1", "f1"),
            paramAsIdentifier("n2", "f1*"),
            paramAsPattern("n3", "f.1*"),
            paramAsPattern("n4", "*"),
            paramAsConstant("n5", "esql"),
            paramAsIdentifier("n_6", "null"),
            paramAsConstant("n7_", "f.1.1")
        );
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
                %s""", query, columnar, timeZone.getId(), locale.toLanguageTag(), filter, paramsString);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(timeZone, request.timeZone());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(params.size(), request.params().size());

        for (int i = 0; i < request.params().size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testInvalidParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        // invalid named parameter for constants
        String paramsString1 = """
            "params":[ {"1" : "v1" }, {"1x" : "v1" }, {"@a" : "v1" }, {"@-#" : "v1" }, 1, 2, {"_1" : "v1" }, {"Å" : 0}, {"x " : 0}]""";
        String json1 = String.format(Locale.ROOT, """
            {
                %s
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
            }""", paramsString1, query, columnar, timeZone.getId(), locale.toLanguageTag(), filter);

        Exception e1 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json1));
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "Failed to parse params: [2:16] [1] is not a valid parameter name, "
                    + "a valid parameter name starts with a letter or underscore, and contains letters, digits and underscores only"
            )
        );
        assertThat(e1.getCause().getMessage(), containsString("[2:31] [1x] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:47] [@a] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:63] [@-#] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:102] [Å] is not a valid parameter name"));
        assertThat(e1.getCause().getMessage(), containsString("[2:113] [x ] is not a valid parameter name"));

        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "Params cannot contain both named and unnamed parameters; "
                    + "got [{1:v1}, {1x:v1}, {@a:v1}, {@-#:v1}, {_1:v1}, {Å:0}, {x :0}] and [{1}, {2}]"
            )
        );

        String paramsString2 = """
            "params":[ 1, 2, {"1" : "v1" }, {"1x" : "v1" }]""";
        String json2 = String.format(Locale.ROOT, """
            {
                %s
                "query": "%s",
                "columnar": %s,
                "locale": "%s",
                "filter": %s
            }""", paramsString2, query, columnar, locale.toLanguageTag(), filter);

        Exception e2 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json2));
        assertThat(
            e2.getCause().getMessage(),
            containsString(
                "Failed to parse params: [2:22] [1] is not a valid parameter name, "
                    + "a valid parameter name starts with a letter or underscore, and contains letters, digits and underscores only"
            )
        );
        assertThat(e2.getCause().getMessage(), containsString("[2:37] [1x] is not a valid parameter name"));
        assertThat(
            e2.getCause().getMessage(),
            containsString("Params cannot contain both named and unnamed parameters; got [{1:v1}, {1x:v1}] and [{1}, {2}]")
        );
    }

    public void testInvalidMultivaluedNamedParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        // invalid named parameter for multivalued constants
        String paramsString = """
            "params":[
             {"_n1": [null, "8.15.0"]}, {"_n2": [null, null, "x"]}, {"_n3": [null, true, false]},
             {"_n4": [null, 1.0, null]}, {"_n5": [null, -799810013, null, 799810013]},
             {"n6" : [{"value" : {"a5" : "v5"}}]}, {"n7" : [{"identifier" : ["x", "y"]}]}, {"n8" : [{"pattern" : ["x*", "y*"]}]}
             ]""";
        String json1 = String.format(Locale.ROOT, """
            {
                %s,
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
            }""", paramsString, query, columnar, timeZone.getId(), locale.toLanguageTag(), filter);

        Exception e1 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json1));
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "[3:2] Parameter [_n1] contains a null entry: [null, 8.15.0]. Null values are not allowed in multivalued params;"
            )
        );
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "[3:29] Parameter [_n2] contains a null entry: [null, null, x]. Null values are not allowed in multivalued params;"
            )
        );
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "[3:57] Parameter [_n3] contains a null entry: [null, true, false]. Null values are not allowed in multivalued params;"
            )
        );
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "[4:2] Parameter [_n4] contains a null entry: [null, 1.0, null]. Null values are not allowed in multivalued params;"
            )
        );
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "[4:30] Parameter [_n5] contains a null entry: [null, -799810013, null, 799810013]. "
                    + "Null values are not allowed in multivalued params;"
            )
        );
        assertThat(e1.getCause().getMessage(), containsString("[5:2] n6=[{value={a5=v5}}] is not supported as a parameter"));
        assertThat(e1.getCause().getMessage(), containsString("[5:40] n7=[{identifier=[x, y]}] is not supported as a parameter"));
        assertThat(e1.getCause().getMessage(), containsString("[5:80] n8=[{pattern=[x*, y*]}] is not supported as a parameter"));
    }

    public void testInvalidMultivaluedUnnamedParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        // invalid named parameter for multivalued constants
        String paramsString = """
            "params":[
             [null, "8.15.0"], [null, null, "x"], [null, true, false], [null, 1.0, null], [null, -799810013, null, 799810013]
             ]""";
        String json1 = String.format(Locale.ROOT, """
            {
                %s,
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
            }""", paramsString, query, columnar, timeZone.getId(), locale.toLanguageTag(), filter);

        Exception e1 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json1));
        assertThat(
            e1.getCause().getMessage(),
            containsString("[3:2] Parameter contains a null entry: [null, 8.15.0]. Null values are not allowed in multivalued params;")
        );
        assertThat(
            e1.getCause().getMessage(),
            containsString("[3:20] Parameter contains a null entry: [null, null, x]. Null values are not allowed in multivalued params;")
        );
        assertThat(
            e1.getCause().getMessage(),
            containsString(
                "[3:39] Parameter contains a null entry: [null, true, false]. Null values are not allowed in multivalued params;"
            )
        );
        assertThat(
            e1.getCause().getMessage(),
            containsString("[3:60] Parameter contains a null entry: [null, 1.0, null]. Null values are not allowed in multivalued params;")
        );
    }

    public void testInvalidParamsString() {
        String query = randomAlphaOfLengthBetween(1, 100);
        String json1 = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "params": {*}
            }""", query);
        Exception e1 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json1));
        String message1 = e1.getCause().getMessage();
        assertThat("Unexpected failure when parsing " + json1 + ". " + message1, containsString("Unexpected token [START_OBJECT]"));
        String json2 = String.format(Locale.ROOT, """
            {
                "query": "%s",
                "params": "foo"
            }""", query);
        Exception e2 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json2));
        String message2 = e2.getCause().getMessage();
        assertThat("Unexpected failure when parsing " + json2 + ". " + message2, containsString("Unexpected token [VALUE_STRING]"));
    }

    public void testInvalidParamsForIdentifiersPatterns() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        // invalid named parameter for identifier and identifier pattern
        String paramsString1 = """
            "params":[{"n1" : {"v" : "v1"}}, {"n2" : {"identifier" : "v2", "pattern" : "v2"}},
            {"n3" : {"identifier" : "v3", "pattern" : "v3"}}, {"n4" : {"pattern" : "v4.1", "value" : "v4.2"}},
            {"n5" : {"value" : {"a5" : "v5"}}},{"n6" : {"identifier" : {"a6.1" : "v6.1", "a6.2" : "v6.2"}}}, {"n7" : {}},
            {"n8" : {"value" : ["x", "y"]}}, {"n9" : {"identifier" : ["x", "y"]}}, {"n10" : {"pattern" : ["x*", "y*"]}},
            {"n11" : {"identifier" : 1}}, {"n12" : {"pattern" : true}}, {"n13" : {"identifier" : null}}, {"n14" : {"pattern" : "v14"}},
            {"n15" : {"pattern" : "v15*"}, "n16" : {"identifier" : "v16"}}]""";
        String json1 = String.format(Locale.ROOT, """
            {
                %s
                "query": "%s",
                "columnar": %s,
                "time_zone": "%s",
                "locale": "%s",
                "filter": %s
            }""", paramsString1, query, columnar, timeZone.getId(), locale.toLanguageTag(), filter);

        Exception e1 = expectThrows(XContentParseException.class, () -> parseEsqlQueryRequestSync(json1));
        String message = e1.getCause().getMessage();
        assertThat(
            message,
            containsString("[2:15] [v] is not a valid param attribute, a valid attribute is any of VALUE, IDENTIFIER, PATTERN; ")
        );
        assertThat(
            message,
            containsString(
                "[2:38] [n2] has multiple param attributes [identifier, pattern], "
                    + "only one of VALUE, IDENTIFIER, PATTERN can be defined in a param;"
            )
        );
        assertThat(
            message,
            containsString(
                "[2:38] [v2] is not a valid value for PATTERN parameter, "
                    + "a valid value for PATTERN parameter is a string and contains *;"
            )
        );
        assertThat(
            message,
            containsString(
                "[3:1] [n3] has multiple param attributes [identifier, pattern], "
                    + "only one of VALUE, IDENTIFIER, PATTERN can be defined in a param;"
            )
        );
        assertThat(
            message,
            containsString(
                "[3:1] [v3] is not a valid value for PATTERN parameter, "
                    + "a valid value for PATTERN parameter is a string and contains *;"
            )
        );
        assertThat(
            message,
            containsString(
                "[3:51] [n4] has multiple param attributes [pattern, value], "
                    + "only one of VALUE, IDENTIFIER, PATTERN can be defined in a param;"
            )
        );
        assertThat(
            message,
            containsString(
                "[3:51] [v4.1] is not a valid value for PATTERN parameter, "
                    + "a valid value for PATTERN parameter is a string and contains *;"
            )
        );
        assertThat(message, containsString("[4:1] n5={value={a5=v5}} is not supported as a parameter;"));
        assertThat(message, containsString("[4:36] [{a6.1=v6.1, a6.2=v6.2}] is not a valid value for IDENTIFIER parameter,"));
        assertThat(message, containsString("a valid value for IDENTIFIER parameter is a string;"));
        assertThat(message, containsString("[4:36] n6={identifier={a6.1=v6.1, a6.2=v6.2}} is not supported as a parameter;"));
        assertThat(
            message,
            containsString("[4:98] [n7] has no valid param attribute, only one of VALUE, IDENTIFIER, PATTERN can be defined in a param;")
        );
        assertThat(
            message,
            containsString("[5:34] n9={identifier=[x, y]} parameter is multivalued, only VALUE parameters can be multivalued;")
        );
        assertThat(
            message,
            containsString("[5:72] n10={pattern=[x*, y*]} parameter is multivalued, only VALUE parameters can be multivalued;")
        );
        assertThat(message, containsString("a valid value for PATTERN parameter is a string and contains *;"));
        assertThat(
            message,
            containsString("[6:1] [1] is not a valid value for IDENTIFIER parameter, a valid value for IDENTIFIER parameter is a string;")
        );
        assertThat(message, containsString("[6:31] [true] is not a valid value for PATTERN parameter,"));
        assertThat(message, containsString("a valid value for PATTERN parameter is a string and contains *;"));
        assertThat(
            message,
            containsString(
                "[6:61] [null] is not a valid value for IDENTIFIER parameter, a valid value for IDENTIFIER parameter is a string;"
            )
        );
        assertThat(message, containsString("[6:94] [v14] is not a valid value for PATTERN parameter,"));
        assertThat(message, containsString("a valid value for PATTERN parameter is a string and contains *;"));
        assertThat(
            message,
            containsString(
                "[7:1] Cannot parse more than one key:value pair as parameter, found [{n16:{identifier=v16}}, {n15:{pattern=v15*}}"
            )
        );
    }

    // Test for https://github.com/elastic/elasticsearch/issues/110028
    public void testNamedParamsMutation() {
        EsqlQueryRequest request1 = new EsqlQueryRequest();
        assertThat(request1.params(), equalTo(new QueryParams()));
        var exceptionMessage = randomAlphaOfLength(10);
        var paramName = randomAlphaOfLength(5);
        var paramValue = randomAlphaOfLength(5);
        request1.params().addParsingError(new ParsingException(Source.EMPTY, exceptionMessage));
        request1.params().addTokenParam(null, paramAsConstant(paramName, paramValue));

        EsqlQueryRequest request2 = new EsqlQueryRequest();
        assertThat(request2.params(), equalTo(new QueryParams()));
    }

    public void testParseFieldsForAsync() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        boolean columnar = randomBoolean();
        ZoneId timeZone = randomZone();
        Locale locale = randomLocale(random());
        QueryBuilder filter = randomQueryBuilder();

        List<QueryParam> params = randomParameters();
        boolean hasParams = params.isEmpty() == false;
        StringBuilder paramsString = paramsString(params, hasParams);
        boolean keepOnCompletion = randomBoolean();
        TimeValue waitForCompletion = randomTimeValue();
        TimeValue keepAlive = randomTimeValue();
        String json = String.format(
            Locale.ROOT,
            """
                {
                    "query": "%s",
                    "columnar": %s,
                    "time_zone": "%s",
                    "locale": "%s",
                    "filter": %s,
                    "keep_on_completion": %s,
                    "wait_for_completion_timeout": "%s",
                    "keep_alive": "%s"
                    %s""",
            query,
            columnar,
            timeZone.getId(),
            locale.toLanguageTag(),
            filter,
            keepOnCompletion,
            waitForCompletion.getStringRep(),
            keepAlive.getStringRep(),
            paramsString
        );

        EsqlQueryRequest request = parseEsqlQueryRequestAsync(json);

        assertEquals(query, request.query());
        assertEquals(columnar, request.columnar());
        assertEquals(timeZone, request.timeZone());
        assertEquals(locale.toLanguageTag(), request.locale().toLanguageTag());
        assertEquals(locale, request.locale());
        assertEquals(filter, request.filter());
        assertEquals(keepOnCompletion, request.keepOnCompletion());
        assertEquals(waitForCompletion, request.waitForCompletionTimeout());
        assertEquals(keepAlive, request.keepAlive());
        assertEquals(params.size(), request.params().size());
        for (int i = 0; i < params.size(); i++) {
            assertEquals(params.get(i), request.params().get(i + 1));
        }
    }

    public void testDefaultValueForOptionalAsyncParams() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 100);
        String json = String.format(Locale.ROOT, """
            {
                "query": "%s"
            }
            """, query);
        EsqlQueryRequest request = parseEsqlQueryRequestAsync(json);
        assertEquals(query, request.query());
        assertFalse(request.keepOnCompletion());
        assertEquals(TimeValue.timeValueSeconds(1), request.waitForCompletionTimeout());
        assertEquals(TimeValue.timeValueDays(5), request.keepAlive());
    }

    public void testRejectUnknownFields() {
        assertParserErrorMessage("""
            {
                "query": "foo",
                "columbar": true
            }""", "unknown field [columbar] did you mean [columnar]?");

        assertParserErrorMessage("""
            {
                "query": "foo",
                "asdf": "Z"
            }""", "unknown field [asdf]");
    }

    public void testMissingQueryIsNotValid() throws IOException {
        String json = """
            {
                "columnar": true
            }""";
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[query] is required"));
    }

    public void testPragmasOnlyValidOnSnapshot() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "pragma": {"foo": "bar"}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        request.onSnapshotBuild(true);
        assertNull(request.validate());

        request.onSnapshotBuild(false);
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[pragma] only allowed in snapshot builds"));

        request.acceptedPragmaRisks(true);
        assertNull(request.validate());
    }

    public void testTablesKeyword() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"keyword": ["a", "b", null, 1, 2.0, ["c", "d"], false]}}}
            }
            """;
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(KEYWORD));
        try (
            BytesRefBlock.Builder builder = new BlockFactory(
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                BigArrays.NON_RECYCLING_INSTANCE
            ).newBytesRefBlockBuilder(10)
        ) {
            builder.appendBytesRef(new BytesRef("a"));
            builder.appendBytesRef(new BytesRef("b"));
            builder.appendNull();
            builder.appendBytesRef(new BytesRef("1"));
            builder.appendBytesRef(new BytesRef("2.0"));
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("c"));
            builder.appendBytesRef(new BytesRef("d"));
            builder.endPositionEntry();
            builder.appendBytesRef(new BytesRef("false"));
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testTablesInteger() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"integer": [1, 2, "3", null, [5, 6]]}}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(INTEGER));
        try (
            IntBlock.Builder builder = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                .newIntBlockBuilder(10)
        ) {
            builder.appendInt(1);
            builder.appendInt(2);
            builder.appendInt(3);
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendInt(5);
            builder.appendInt(6);
            builder.endPositionEntry();
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testTablesLong() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"long": [1, 2, "3", null, [5, 6]]}}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(LONG));
        try (
            LongBlock.Builder builder = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                .newLongBlockBuilder(10)
        ) {
            builder.appendLong(1);
            builder.appendLong(2);
            builder.appendLong(3);
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendLong(5);
            builder.appendLong(6);
            builder.endPositionEntry();
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testTablesDouble() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {"a": {"c": {"double": [1.1, 2, "3.1415", null, [5.1, "-6"]]}}}
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        Column c = request.tables().get("a").get("c");
        assertThat(c.type(), equalTo(DOUBLE));
        try (
            DoubleBlock.Builder builder = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                .newDoubleBlockBuilder(10)
        ) {
            builder.appendDouble(1.1);
            builder.appendDouble(2);
            builder.appendDouble(3.1415);
            builder.appendNull();
            builder.beginPositionEntry();
            builder.appendDouble(5.1);
            builder.appendDouble(-6);
            builder.endPositionEntry();
            assertThat(c.values(), equalTo(builder.build()));
        }
        assertTablesOnlyValidOnSnapshot(request);
    }

    public void testManyTables() throws IOException {
        String json = """
            {
                "query": "ROW x = 1",
                "tables": {
                    "t1": {
                        "a": {"long": [1]},
                        "b": {"long": [1]},
                        "c": {"keyword": [1]},
                        "d": {"long": [1]}
                    },
                    "t2": {
                        "a": {"long": [1]},
                        "b": {"integer": [1]},
                        "c": {"long": [1]},
                        "d": {"long": [1]}
                    }
                }
            }
            """;

        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertThat(request.tables().keySet(), hasSize(2));
        Map<String, Column> t1 = request.tables().get("t1");
        assertThat(t1.get("a").type(), equalTo(LONG));
        assertThat(t1.get("b").type(), equalTo(LONG));
        assertThat(t1.get("c").type(), equalTo(KEYWORD));
        assertThat(t1.get("d").type(), equalTo(LONG));
        Map<String, Column> t2 = request.tables().get("t2");
        assertThat(t2.get("a").type(), equalTo(LONG));
        assertThat(t2.get("b").type(), equalTo(INTEGER));
        assertThat(t2.get("c").type(), equalTo(LONG));
        assertThat(t2.get("d").type(), equalTo(LONG));
        assertTablesOnlyValidOnSnapshot(request);
    }

    private void assertTablesOnlyValidOnSnapshot(EsqlQueryRequest request) {
        request.onSnapshotBuild(true);
        assertNull(request.validate());

        request.onSnapshotBuild(false);
        assertNotNull(request.validate());
        assertThat(request.validate().getMessage(), containsString("[tables] only allowed in snapshot builds"));
    }

    public void testTask() throws IOException {
        String query = randomAlphaOfLength(10);
        int id = randomInt();

        String requestJson = """
            {
                "query": "QUERY"
            }""".replace("QUERY", query);

        EsqlQueryRequest request = parseEsqlQueryRequestSync(requestJson);
        String localNode = randomAlphaOfLength(2);
        Task task = request.createTask(new TaskId(localNode, id), "transport", EsqlQueryAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
        assertThat(task.getDescription(), equalTo(query));

        TaskInfo taskInfo = task.taskInfo(localNode, true);
        String json = taskInfo.toString();
        String expected = Strings.format(
            """
                {
                  "node" : "%s",
                  "id" : %d,
                  "type" : "transport",
                  "action" : "indices:data/read/esql",
                  "status" : {
                    "request_id" : "%s"
                  },
                  "description" : "%s",
                  "start_time" : "%s",
                  "start_time_in_millis" : %d,
                  "running_time" : "%s",
                  "running_time_in_nanos" : %d,
                  "cancellable" : true,
                  "cancelled" : false,
                  "headers" : { }
                }
                """.trim(),
            localNode,
            id,
            ((EsqlQueryStatus) taskInfo.status()).id().getEncoded(),
            query,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(taskInfo.startTime()),
            taskInfo.startTime(),
            TimeValue.timeValueNanos(taskInfo.runningTimeNanos()).toString(),
            taskInfo.runningTimeNanos()
        );
        assertThat(json, equalTo(expected));
    }

    public void testProjectRouting() throws IOException {
        String json = """
            {
                "query": "FROM test",
                "project_routing": "_alias:_origin"
            }""";
        EsqlQueryRequest request = parseEsqlQueryRequest(json, randomBoolean());
        assertThat(request.projectRouting(), is("_alias:_origin"));
    }

    private List<QueryParam> randomParameters() {
        if (randomBoolean()) {
            return Collections.emptyList();
        } else {
            int len = randomIntBetween(1, 10);
            List<QueryParam> arr = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                @SuppressWarnings("unchecked")
                Supplier<QueryParam> supplier = randomFrom(
                    () -> paramAsConstant(null, randomBoolean()),
                    () -> paramAsConstant(null, randomInt()),
                    () -> paramAsConstant(null, randomLong()),
                    () -> paramAsConstant(null, randomDouble()),
                    () -> paramAsConstant(null, null),
                    () -> paramAsConstant(null, randomAlphaOfLength(10))
                );
                arr.add(supplier.get());
            }
            return Collections.unmodifiableList(arr);
        }
    }

    private StringBuilder paramsString(List<QueryParam> params, boolean hasParams) {
        StringBuilder paramsString = new StringBuilder();
        if (hasParams) {
            paramsString.append(",\"params\":[");
            boolean first = true;
            for (QueryParam param : params) {
                if (first == false) {
                    paramsString.append(", ");
                }
                first = false;
                if (param.type() == KEYWORD) {
                    paramsString.append("\"");
                    paramsString.append(param.value());
                    paramsString.append("\"");
                } else if (param.type().isNumeric() || param.type() == BOOLEAN || param.type() == NULL) {
                    paramsString.append(param.value());
                }
            }
            paramsString.append("]}");
        } else {
            paramsString.append("}");
        }
        return paramsString;
    }

    private static void assertParserErrorMessage(String json, String message) {
        Exception e = expectThrows(IllegalArgumentException.class, () -> parseEsqlQueryRequestSync(json));
        assertThat(e.getMessage(), containsString(message));

        e = expectThrows(IllegalArgumentException.class, () -> parseEsqlQueryRequestAsync(json));
        assertThat(e.getMessage(), containsString(message));
    }

    static EsqlQueryRequest parseEsqlQueryRequest(String json, boolean sync) throws IOException {
        return sync ? parseEsqlQueryRequestSync(json) : parseEsqlQueryRequestAsync(json);
    }

    static EsqlQueryRequest parseEsqlQueryRequestSync(String json) throws IOException {
        var request = parseEsqlQueryRequest(json, RequestXContent::parseSync);
        assertFalse(request.async());
        return request;
    }

    static EsqlQueryRequest parseEsqlQueryRequestAsync(String json) throws IOException {
        var request = parseEsqlQueryRequest(json, RequestXContent::parseAsync);
        assertTrue(request.async());
        return request;
    }

    static EsqlQueryRequest parseEsqlQueryRequest(String json, Function<XContentParser, EsqlQueryRequest> fromXContentFunc)
        throws IOException {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(
            new NamedXContentRegistry(searchModule.getNamedXContents())
        );
        try (XContentParser parser = XContentType.JSON.xContent().createParser(config, json)) {
            return fromXContentFunc.apply(parser);
        }
    }

    public static QueryBuilder randomQueryBuilder() {
        return randomFrom(
            new TermQueryBuilder(randomAlphaOfLength(5), randomAlphaOfLengthBetween(1, 10)),
            new RangeQueryBuilder(randomAlphaOfLength(5)).gt(randomIntBetween(0, 1000))
        );
    }
}
