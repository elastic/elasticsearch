/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex.resumeinfo;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.BulkByScrollTaskStatusTests;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wire serialization tests for {@link ResumeInfo.ScrollWorkerResumeInfo}.
 */
public class ScrollWorkerResumeInfoWireSerializingTests extends AbstractWireSerializingTestCase<ResumeInfo.ScrollWorkerResumeInfo> {

    /**
     * Register {@link ResumeInfo.WorkerResumeInfo} and {@link Task.Status} so that
     * {@link ResumeInfo.ScrollWorkerResumeInfo} and its status field can be deserialized by name.
     */
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(org.elasticsearch.cluster.ClusterModule.getNamedWriteables());
        entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, BulkByScrollTask.Status.NAME, BulkByScrollTask.Status::new));
        entries.add(
            new NamedWriteableRegistry.Entry(
                ResumeInfo.WorkerResumeInfo.class,
                ResumeInfo.ScrollWorkerResumeInfo.NAME,
                ResumeInfo.ScrollWorkerResumeInfo::new
            )
        );
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected Writeable.Reader<ResumeInfo.ScrollWorkerResumeInfo> instanceReader() {
        return ResumeInfo.ScrollWorkerResumeInfo::new;
    }

    @Override
    protected ResumeInfo.ScrollWorkerResumeInfo createTestInstance() {
        return randomScrollWorkerResumeInfo();
    }

    @Override
    protected ResumeInfo.ScrollWorkerResumeInfo mutateInstance(ResumeInfo.ScrollWorkerResumeInfo scrollWorkerResumeInfo)
        throws IOException {
        // ScrollWorkerResumeInfo is a record; no need to verify equality via mutations
        return null;
    }

    public static ResumeInfo.ScrollWorkerResumeInfo randomScrollWorkerResumeInfo() {
        return new ResumeInfo.ScrollWorkerResumeInfo(
            randomAlphaOfLengthBetween(1, 20),
            randomLong(),
            BulkByScrollTaskStatusTests.randomStatusWithoutException(),
            randomBoolean() ? null : (randomBoolean() ? Version.CURRENT : Version.fromId(randomIntBetween(1, 999999)))
        );
    }
}
