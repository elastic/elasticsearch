/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ql.TestUtils.randomRuntimeMappings;
import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilter;
import static org.elasticsearch.xpack.sql.action.SqlTestUtils.randomFilterOrNull;

public class SqlTranslateRequestTests extends AbstractSerializingTestCase<SqlTranslateRequest> {

    public Mode testMode;

    @Before
    public void setup() {
        testMode = randomFrom(Mode.values());
    }

    @Override
    protected SqlTranslateRequest createTestInstance() {
        return new SqlTranslateRequest(randomAlphaOfLength(10), emptyList(), randomFilterOrNull(random()),
                randomRuntimeMappings(),randomZone(), between(1, Integer.MAX_VALUE), randomTV(), randomTV(), new RequestInfo(testMode));
    }

    @Override
    protected Writeable.Reader<SqlTranslateRequest> instanceReader() {
        return SqlTranslateRequest::new;
    }

    private TimeValue randomTV() {
        return TimeValue.parseTimeValue(randomTimeValue(), null, "test");
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected SqlTranslateRequest doParseInstance(XContentParser parser) {
        return SqlTranslateRequest.fromXContent(parser);
    }

    @Override
    protected SqlTranslateRequest mutateInstance(SqlTranslateRequest instance) throws IOException {
        @SuppressWarnings("unchecked")
        Consumer<SqlTranslateRequest> mutator = randomFrom(
                request -> request.query(randomValueOtherThan(request.query(), () -> randomAlphaOfLength(5))),
                request -> request.zoneId(randomValueOtherThan(request.zoneId(), ESTestCase::randomZone)),
                request -> request.fetchSize(randomValueOtherThan(request.fetchSize(), () -> between(1, Integer.MAX_VALUE))),
                request -> request.requestTimeout(randomValueOtherThan(request.requestTimeout(), this::randomTV)),
                request -> request.filter(randomValueOtherThan(request.filter(),
                        () -> request.filter() == null ? randomFilter(random()) : randomFilterOrNull(random()))),
                request -> request.runtimeMappings(randomValueOtherThan(request.runtimeMappings(), () -> randomRuntimeMappings()))
        );
        SqlTranslateRequest newRequest = new SqlTranslateRequest(instance.query(), instance.params(), instance.filter(),
                instance.runtimeMappings(), instance.zoneId(), instance.fetchSize(), instance.requestTimeout(), instance.pageTimeout(),
                instance.requestInfo());
        mutator.accept(newRequest);
        return newRequest;
    }
}
