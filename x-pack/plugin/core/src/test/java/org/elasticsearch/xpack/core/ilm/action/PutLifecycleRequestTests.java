/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class PutLifecycleRequestTests extends AbstractXContentSerializingTestCase<PutLifecycleRequest> {

    private String lifecycleName;

    @Before
    public void setup() {
        lifecycleName = randomAlphaOfLength(20);
    }

    @Override
    protected PutLifecycleRequest createTestInstance() {
        return new PutLifecycleRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            LifecyclePolicyTests.randomTimeseriesLifecyclePolicy(lifecycleName)
        );
    }

    @Override
    protected Writeable.Reader<PutLifecycleRequest> instanceReader() {
        return PutLifecycleRequest::new;
    }

    @Override
    protected PutLifecycleRequest doParseInstance(XContentParser parser) {
        return PutLifecycleRequest.parseRequest(new PutLifecycleRequest.Factory() {
            @Override
            public PutLifecycleRequest create(LifecyclePolicy lifecyclePolicy) {
                return new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, lifecyclePolicy);
            }

            @Override
            public String getPolicyName() {
                return lifecycleName;
            }
        }, parser);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    LifecycleType.class,
                    TimeseriesLifecycleType.TYPE,
                    (in) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, AllocateAction.NAME, AllocateAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, WaitForSnapshotAction.NAME, WaitForSnapshotAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::readFrom),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ForceMergeAction.NAME, ForceMergeAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ReadOnlyAction.NAME, ReadOnlyAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, RolloverAction.NAME, RolloverAction::read),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, ShrinkAction.NAME, ShrinkAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, FreezeAction.NAME, in -> FreezeAction.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, SetPriorityAction.NAME, SetPriorityAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, UnfollowAction.NAME, in -> UnfollowAction.INSTANCE),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, MigrateAction.NAME, MigrateAction::readFrom),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, SearchableSnapshotAction.NAME, SearchableSnapshotAction::new),
                new NamedWriteableRegistry.Entry(LifecycleAction.class, DownsampleAction.NAME, DownsampleAction::new)
            )
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(
            List.of(
                new NamedXContentRegistry.Entry(
                    LifecycleType.class,
                    new ParseField(TimeseriesLifecycleType.TYPE),
                    (p) -> TimeseriesLifecycleType.INSTANCE
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(WaitForSnapshotAction.NAME),
                    WaitForSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
                new NamedXContentRegistry.Entry(
                    LifecycleAction.class,
                    new ParseField(SearchableSnapshotAction.NAME),
                    SearchableSnapshotAction::parse
                ),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MigrateAction.NAME), MigrateAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DownsampleAction.NAME), DownsampleAction::parse)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected PutLifecycleRequest mutateInstance(PutLifecycleRequest request) {
        String name = randomBoolean() ? lifecycleName : randomAlphaOfLength(5);
        LifecyclePolicy policy = randomValueOtherThan(
            request.getPolicy(),
            () -> LifecyclePolicyTests.randomTimeseriesLifecyclePolicy(name)
        );
        return new PutLifecycleRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, policy);
    }

}
