/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.action.SqlTranslateResponse;

import java.io.IOException;

public class SqlTranslateResponseTests extends AbstractStreamableTestCase<SqlTranslateResponse> {

    @Override
    protected SqlTranslateResponse createTestInstance() {
        SearchSourceBuilder s = new SearchSourceBuilder();
        if (randomBoolean()) {
            long docValues = iterations(5, 10);
            for (int i = 0; i < docValues; i++) {
                s.docValueField(randomAlphaOfLength(10), DocValueFieldsContext.USE_DEFAULT_FORMAT);
            }
        }

        if (randomBoolean()) {
            long sourceFields = iterations(5, 10);
            for (int i = 0; i < sourceFields; i++) {
                s.storedField(randomAlphaOfLength(10));
            }
        }

        s.fetchSource(randomBoolean()).from(randomInt(256)).explain(randomBoolean()).size(randomInt(256));

        return new SqlTranslateResponse(s);
    }

    @Override
    protected SqlTranslateResponse createBlankInstance() {
        return new SqlTranslateResponse();
    }

    @Override
    protected SqlTranslateResponse mutateInstance(SqlTranslateResponse instance) throws IOException {
        SqlTranslateResponse sqlTranslateResponse = copyInstance(instance);
        SearchSourceBuilder source = sqlTranslateResponse.source();
        source.size(randomValueOtherThan(source.size(), () -> between(0, Integer.MAX_VALUE)));
        return new SqlTranslateResponse(source);
    }
}
