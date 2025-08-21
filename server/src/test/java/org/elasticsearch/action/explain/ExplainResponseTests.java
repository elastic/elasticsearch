/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.explain;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.get.GetResultTests;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;

public class ExplainResponseTests extends AbstractXContentSerializingTestCase<ExplainResponse> {

    private static final ConstructingObjectParser<ExplainResponse, Boolean> PARSER = new ConstructingObjectParser<>(
        "explain",
        true,
        (arg, exists) -> new ExplainResponse((String) arg[0], (String) arg[1], exists, (Explanation) arg[2], (GetResult) arg[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ExplainResponse._INDEX);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ExplainResponse._ID);
        final ConstructingObjectParser<Explanation, Boolean> explanationParser = getExplanationsParser();
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), explanationParser, ExplainResponse.EXPLANATION);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> GetResultTests.parseInstanceFromEmbedded(p),
            ExplainResponse.GET
        );
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<Explanation, Boolean> getExplanationsParser() {
        final ConstructingObjectParser<Explanation, Boolean> explanationParser = new ConstructingObjectParser<>(
            "explanation",
            true,
            arg -> {
                if ((float) arg[0] > 0) {
                    return Explanation.match((float) arg[0], (String) arg[1], (Collection<Explanation>) arg[2]);
                } else {
                    return Explanation.noMatch((String) arg[1], (Collection<Explanation>) arg[2]);
                }
            }
        );
        explanationParser.declareFloat(ConstructingObjectParser.constructorArg(), ExplainResponse.VALUE);
        explanationParser.declareString(ConstructingObjectParser.constructorArg(), ExplainResponse.DESCRIPTION);
        explanationParser.declareObjectArray(ConstructingObjectParser.constructorArg(), explanationParser, ExplainResponse.DETAILS);
        return explanationParser;
    }

    @Override
    protected ExplainResponse doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, randomBoolean());
    }

    @Override
    protected Writeable.Reader<ExplainResponse> instanceReader() {
        return ExplainResponse::new;
    }

    @Override
    protected ExplainResponse createTestInstance() {
        String index = randomAlphaOfLength(5);
        String id = String.valueOf(randomIntBetween(1, 100));
        boolean exist = randomBoolean();
        Explanation explanation = randomExplanation(randomExplanation(randomExplanation()), randomExplanation());
        String fieldName = randomAlphaOfLength(10);
        List<Object> values = Arrays.asList(randomAlphaOfLengthBetween(3, 10), randomInt(), randomLong(), randomDouble(), randomBoolean());
        GetResult getResult = new GetResult(
            randomAlphaOfLengthBetween(3, 10),
            randomAlphaOfLengthBetween(3, 10),
            0,
            1,
            randomNonNegativeLong(),
            true,
            RandomObjects.randomSource(random()),
            singletonMap(fieldName, new DocumentField(fieldName, values)),
            null
        );
        return new ExplainResponse(index, id, exist, explanation, getResult);
    }

    @Override
    protected ExplainResponse mutateInstance(ExplainResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("get") || field.startsWith("get.fields") || field.startsWith("get._source");
    }

    public void testToXContent() throws IOException {
        String index = "index";
        String id = "1";
        boolean exist = true;
        Explanation explanation = Explanation.match(1.0f, "description", Collections.emptySet());
        GetResult getResult = new GetResult(
            null,
            null,
            0,
            1,
            -1,
            true,
            new BytesArray("""
                { "field1" : "value1", "field2":"value2"}"""),
            singletonMap("field1", new DocumentField("field1", singletonList("value1"))),
            null
        );
        ExplainResponse response = new ExplainResponse(index, id, exist, explanation, getResult);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String generatedResponse = BytesReference.bytes(builder).utf8ToString().replaceAll("\\s+", "");

        String expectedResponse = ("""
            {
                "_index": "index",
                "_id": "1",
                "matched": true,
                "explanation": {
                    "value": 1.0,
                    "description": "description",
                    "details": []
                },
                "get": {
                    "_seq_no": 0,
                    "_primary_term": 1,
                    "found": true,
                    "_source": {
                        "field1": "value1",
                        "field2": "value2"
                    },
                    "fields": {
                        "field1": [
                          "value1"
                        ]
                    }
                }
            }
            """).replaceAll("\\s+", "");
        assertThat(expectedResponse, equalTo(generatedResponse));
    }

    private static Explanation randomExplanation(Explanation... explanations) {
        return Explanation.match(
            randomFloat(),
            randomAlphaOfLengthBetween(1, 10),
            explanations.length > 0 ? explanations : new Explanation[0]
        );
    }
}
