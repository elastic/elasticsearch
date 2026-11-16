/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class QueryExplanationTests extends AbstractXContentSerializingTestCase<QueryExplanation> {

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
    protected QueryExplanation mutateInstance(QueryExplanation instance) {
        String index = instance.getIndex();
        int shard = instance.getShard();
        boolean valid = instance.isValid();
        String explanation = instance.getExplanation();
        String error = instance.getError();
        switch (randomIntBetween(0, 4)) {
            case 0 -> index = randomValueOtherThan(index, () -> "index_" + randomInt(1000));
            case 1 -> shard = randomValueOtherThan(shard, () -> randomInt(100));
            case 2 -> valid = valid == false;
            case 3 -> explanation = randomValueOtherThan(explanation, () -> randomAlphaOfLength(randomIntBetween(10, 100)));
            default -> error = randomValueOtherThan(error, () -> randomAlphaOfLength(randomIntBetween(10, 100)));
        }
        return new QueryExplanation(index, shard, valid, explanation, error);
    }

    @Override
    protected Writeable.Reader<QueryExplanation> instanceReader() {
        return QueryExplanation::new;
    }
}
