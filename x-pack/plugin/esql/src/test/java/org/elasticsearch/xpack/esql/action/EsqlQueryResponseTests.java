/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EsqlQueryResponseTests extends AbstractXContentSerializingTestCase<EsqlQueryResponse> {

    @Override
    protected EsqlQueryResponse createTestInstance() {
        int noCols = randomIntBetween(1, 10);
        List<ColumnInfo> columns = randomList(noCols, noCols, this::randomColumnInfo);
        int noRows = randomIntBetween(1, 20);
        List<List<Object>> values = randomList(noRows, noRows, () -> randomRow(noCols));
        // columnar param can't be different from the default value (false) since the EsqlQueryResponse will be serialized (by some random
        // XContentType, not to a StreamOutput) and parsed back, which doesn't preserve columnar field's value.
        return new EsqlQueryResponse(columns, values, false);
    }

    private List<Object> randomRow(int noCols) {
        return randomList(noCols, noCols, ESTestCase::randomInt);
    }

    private ColumnInfo randomColumnInfo() {
        return new ColumnInfo(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected EsqlQueryResponse mutateInstance(EsqlQueryResponse instance) throws IOException {
        EsqlQueryResponse newInstance = new EsqlQueryResponse(
            new ArrayList<>(instance.columns()),
            new ArrayList<>(instance.values()),
            instance.columnar() == false
        );

        int modCol = randomInt(instance.columns().size() - 1);
        newInstance.columns().set(modCol, randomColumnInfo());

        int modRow = randomInt(instance.values().size() - 1);
        newInstance.values().set(modRow, randomRow(instance.columns().size()));

        return newInstance;
    }

    @Override
    protected Writeable.Reader<EsqlQueryResponse> instanceReader() {
        return EsqlQueryResponse::new;
    }

    @Override
    protected EsqlQueryResponse doParseInstance(XContentParser parser) throws IOException {
        return EsqlQueryResponse.fromXContent(parser);
    }
}
