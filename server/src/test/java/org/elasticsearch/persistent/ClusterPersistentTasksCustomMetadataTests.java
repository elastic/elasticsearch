/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.persistent;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.Metadata.ClusterCustom;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.State;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestParams;
import org.elasticsearch.persistent.TestPersistentTasksPlugin.TestPersistentTasksExecutor;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.Arrays;

public class ClusterPersistentTasksCustomMetadataTests extends BasePersistentTasksCustomMetadataTests<ClusterCustom> {

    @Override
    protected Writeable.Reader<ClusterCustom> instanceReader() {
        return ClusterPersistentTasksCustomMetadata::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(
                new Entry(ClusterCustom.class, ClusterPersistentTasksCustomMetadata.TYPE, ClusterPersistentTasksCustomMetadata::new),
                new Entry(NamedDiff.class, ClusterPersistentTasksCustomMetadata.TYPE, ClusterPersistentTasksCustomMetadata::readDiffFrom),
                new Entry(PersistentTaskParams.class, TestPersistentTasksExecutor.NAME, TestParams::new),
                new Entry(PersistentTaskState.class, TestPersistentTasksExecutor.NAME, State::new)
            )
        );
    }

    @Override
    protected Writeable.Reader<Diff<ClusterCustom>> diffReader() {
        return ClusterPersistentTasksCustomMetadata::readDiffFrom;
    }

    @Override
    protected ClusterPersistentTasksCustomMetadata doParseInstance(XContentParser parser) {
        return ClusterPersistentTasksCustomMetadata.fromXContent(parser);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    PersistentTaskParams.class,
                    new ParseField(TestPersistentTasksExecutor.NAME),
                    TestParams::fromXContent
                ),
                new NamedXContentRegistry.Entry(
                    PersistentTaskState.class,
                    new ParseField(TestPersistentTasksExecutor.NAME),
                    State::fromXContent
                )
            )
        );
    }

    @Override
    protected PersistentTasks.Builder<?> builder() {
        return ClusterPersistentTasksCustomMetadata.builder();
    }

    @Override
    protected PersistentTasks.Builder<?> builder(ClusterCustom testInstance) {
        return ClusterPersistentTasksCustomMetadata.builder((ClusterPersistentTasksCustomMetadata) testInstance);
    }
}
