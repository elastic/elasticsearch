/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.client.ilm.LifecyclePolicyTests.createRandomPolicy;

public class LifecyclePolicyMetadataTests extends AbstractXContentTestCase<LifecyclePolicyMetadata> {

    private String policyName;

    @Override
    protected LifecyclePolicyMetadata createTestInstance() {
        policyName = randomAlphaOfLengthBetween(5,20);
        LifecyclePolicy policy = createRandomPolicy(policyName);
        return new LifecyclePolicyMetadata(policy, randomLong(), randomLong());
    }

    @Override
    protected LifecyclePolicyMetadata doParseInstance(XContentParser parser) throws IOException {
        return LifecyclePolicyMetadata.parse(parser, policyName);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return (field) ->
            // phases is a list of Phase parsable entries only
            field.endsWith(".phases")
            // these are all meant to be maps of strings, so complex objects will confuse the parser
            || field.endsWith(".include")
            || field.endsWith(".exclude")
            || field.endsWith(".require")
            // actions are meant to be a list of LifecycleAction parsable entries only
            || field.endsWith(".actions");

    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.addAll(Arrays.asList(
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(AllocateAction.NAME), AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ForceMergeAction.NAME), ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ReadOnlyAction.NAME), ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(RolloverAction.NAME), RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(ShrinkAction.NAME), ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(WaitForSnapshotAction.NAME),
                WaitForSnapshotAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(FreezeAction.NAME), FreezeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SetPriorityAction.NAME), SetPriorityAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(SearchableSnapshotAction.NAME),
                SearchableSnapshotAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(UnfollowAction.NAME), UnfollowAction::parse)
        ));
        return new NamedXContentRegistry(entries);
    }
}
