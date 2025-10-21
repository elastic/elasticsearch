/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.Utils.modifiableMap;
import static org.hamcrest.Matchers.is;

public class QueryParametersTests extends AbstractBWCWireSerializationTestCase<QueryParameters> {
    public static QueryParameters createRandom() {
        var parameters = randomList(5, () -> new QueryParameters.Parameter(randomAlphaOfLength(5), randomAlphaOfLength(5)));
        return new QueryParameters(parameters);
    }

    public void testFromMap() {
        Map<String, Object> params = new HashMap<>(Map.of(QueryParameters.QUERY_PARAMETERS, List.of(List.of("test_key", "test_value"))));

        assertThat(
            QueryParameters.fromMap(params, new ValidationException()),
            is(new QueryParameters(List.of(new QueryParameters.Parameter("test_key", "test_value"))))
        );
    }

    public void testFromMap_ReturnsEmpty_IfFieldDoesNotExist() {
        assertThat(QueryParameters.fromMap(modifiableMap(Map.of()), new ValidationException()), is(QueryParameters.EMPTY));
    }

    public void testFromMap_Throws_IfFieldIsInvalid() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> QueryParameters.fromMap(modifiableMap(Map.of(QueryParameters.QUERY_PARAMETERS, "string")), validation)
        );

        assertThat(
            exception.getMessage(),
            is(
                "Validation Failed: 1: field [query_parameters] is not of the expected type. "
                    + "The value [string] cannot be converted to a [List];"
            )
        );
    }

    public void testXContent() throws IOException {
        var entity = new QueryParameters(List.of(new QueryParameters.Parameter("test_key", "test_value")));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        {
            builder.startObject();
            entity.toXContent(builder, null);
            builder.endObject();
        }
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "query_parameters": [
                    ["test_key", "test_value"]
                ]
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testXContent_EmptyParameters() throws IOException {
        var entity = QueryParameters.EMPTY;

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(""));
    }

    @Override
    protected Writeable.Reader<QueryParameters> instanceReader() {
        return QueryParameters::new;
    }

    @Override
    protected QueryParameters createTestInstance() {
        return createRandom();
    }

    @Override
    protected QueryParameters mutateInstance(QueryParameters instance) {
        return randomValueOtherThan(instance, QueryParametersTests::createRandom);
    }

    @Override
    protected QueryParameters mutateInstanceForVersion(QueryParameters instance, TransportVersion version) {
        return instance;
    }
}
