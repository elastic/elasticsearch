/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.textstructure.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.HashMap;
import java.util.Map;


public class FindFieldStructureActionRequestTests extends AbstractWireSerializingTestCase<FindFieldStructureAction.Request> {

    @Override
    protected FindFieldStructureAction.Request createTestInstance() {

        FindFieldStructureAction.Request request = new FindFieldStructureAction.Request();

        if (randomBoolean()) {
            request.setLinesToSample(randomIntBetween(10, 2000));
        }

        if (randomBoolean()) {
            request.setLineMergeSizeLimit(randomIntBetween(1000, 20000));
        }

        if (randomBoolean()) {
            request.setCharset(randomAlphaOfLength(10));
        }

        if (randomBoolean()) {
            TextStructure.Format format = randomFrom(TextStructure.Format.values());
            request.setFormat(format);
            if (format == TextStructure.Format.DELIMITED) {
                if (randomBoolean()) {
                    request.setColumnNames(generateRandomStringArray(10, 15, false, false));
                }
                if (randomBoolean()) {
                    request.setHasHeaderRow(randomBoolean());
                }
                if (randomBoolean()) {
                    request.setDelimiter(randomFrom(',', '\t', ';', '|'));
                }
                if (randomBoolean()) {
                    request.setQuote(randomFrom('"', '\''));
                }
                if (randomBoolean()) {
                    request.setShouldTrimFields(randomBoolean());
                }
            } else if (format == TextStructure.Format.SEMI_STRUCTURED_TEXT) {
                if (randomBoolean()) {
                    request.setGrokPattern(randomAlphaOfLength(80));
                }
            }
        }

        if (randomBoolean()) {
            request.setTimestampFormat(randomAlphaOfLength(20));
        }
        if (randomBoolean()) {
            request.setTimestampField(randomAlphaOfLength(15));
        }

        request.setIndices(randomArray(1, 10, String[]::new, () -> randomAlphaOfLength(10)));
        request.setFieldName(randomAlphaOfLength(10));
        if (randomBoolean()) {
            request.setQueryBuilder(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("foo", "bar")));
        }
        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("type", "keyword");
            settings.put("script", "");
            Map<String, Object> field = new HashMap<>();
            field.put("runtime_field_foo", settings);
            request.setRuntimeMappings(field);
        }

        return request;
    }

    @Override
    protected Writeable.Reader<FindFieldStructureAction.Request> instanceReader() {
        return FindFieldStructureAction.Request::new;
    }

}
