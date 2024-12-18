/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.migrate.action.GetMigrationReindexStatusAction.Response;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamEnrichedStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GetMigrationReindexStatusActionResponseTests extends AbstractWireSerializingTestCase<Response> {
    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return new Response(getRandomStatus());
    }

    @Override
    protected Response mutateInstance(Response instance) throws IOException {
        return createTestInstance(); // There's only one field
    }

    private ReindexDataStreamEnrichedStatus getRandomStatus() {
        return new ReindexDataStreamEnrichedStatus(
            randomLong(),
            randomNegativeInt(),
            randomNegativeInt(),
            randomBoolean(),
            nullableTestException(),
            randomInProgressMap(),
            randomNegativeInt(),
            randomErrorList()
        );
    }

    private Map<String, Tuple<Long, Long>> randomInProgressMap() {
        return randomMap(1, 50, () -> Tuple.tuple(randomAlphaOfLength(50), Tuple.tuple(randomNonNegativeLong(), randomNonNegativeLong())));
    }

    private Exception nullableTestException() {
        if (randomBoolean()) {
            return testException();
        }
        return null;
    }

    private Exception testException() {
        /*
         * Unfortunately ElasticsearchException doesn't have an equals and just falls back to Object::equals. So we can't test for equality
         * when we're using an exception. So always just use null.
         */
        return null;
    }

    private List<Tuple<String, Exception>> randomErrorList() {
        return randomErrorList(0);
    }

    private List<Tuple<String, Exception>> randomErrorList(int minSize) {
        return randomList(minSize, Math.max(minSize, 100), () -> Tuple.tuple(randomAlphaOfLength(30), testException()));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
        // return new NamedWriteableRegistry(List.of(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME,
        // RawTaskStatus::new)));
    }
}
