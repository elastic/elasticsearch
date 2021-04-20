/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class QueryExplanationTests extends AbstractSerializingTestCase<QueryExplanation> {

    static QueryExplanation createRandomQueryExplanation(boolean isValid) {
        String index = "index_" + randomInt(1000);
        int shard = randomInt(100);
        Boolean valid = isValid;
        String errorField = null;
        if (valid == false) {
            errorField = randomAlphaOfLength(randomIntBetween(10, 100));
        }
        String explanation = randomAlphaOfLength(randomIntBetween(10, 100));
        return new QueryExplanation(index, shard, valid, explanation, errorField);
    }

    static QueryExplanation createRandomQueryExplanation() {
        return createRandomQueryExplanation(randomBoolean());
    }

    @Override
    protected QueryExplanation doParseInstance(XContentParser parser) throws IOException {
        return QueryExplanation.fromXContent(parser);
    }

    @Override
    protected QueryExplanation createTestInstance() {
        return createRandomQueryExplanation();
    }

    @Override
    protected Writeable.Reader<QueryExplanation> instanceReader() {
        return QueryExplanation::new;
    }
}
