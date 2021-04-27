/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class SqlManageAsyncRequestTests extends AbstractWireSerializingTestCase<SqlManageAsyncRequest> {

    public void testXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance,
            (instance, xbuilder) -> instance.toXContent(xbuilder, ToXContent.EMPTY_PARAMS), SqlManageAsyncRequest::fromXContent)
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(false)
            .shuffleFieldsExceptions(Strings.EMPTY_ARRAY)
            .randomFieldsExcludeFilter(field -> false)
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(true)
            .test();
    }

    @Override
    protected Writeable.Reader<SqlManageAsyncRequest> instanceReader() {
        return SqlManageAsyncRequest::new;
    }

    @Override
    protected SqlManageAsyncRequest createTestInstance() {
        return new SqlManageAsyncRequest(randomRequestInfo(), randomAlphaOfLength(100));
    }

    @Override
    protected SqlManageAsyncRequest mutateInstance(SqlManageAsyncRequest original) {
       return randomBoolean()
           ? new SqlManageAsyncRequest(randomValueOtherThan(original.requestInfo(), this::randomRequestInfo), original.id())
           : new SqlManageAsyncRequest(original.requestInfo(), randomValueOtherThan(original.id(), () -> randomAlphaOfLength(100)));
    }

    private RequestInfo randomRequestInfo() {
        String randomVersion = randomFrom(Version.getDeclaredVersions(Version.class)).toString();
        return new RequestInfo(randomFrom(Mode.values()), randomAlphaOfLength(10), randomVersion);
    }


}
