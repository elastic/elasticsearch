/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;

import java.util.Arrays;
import java.util.List;

public class IndexLifecycleNamedXContentProvider implements NamedXContentProvider {


    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            // ILM
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(AllocateAction.NAME),
                AllocateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(DeleteAction.NAME),
                DeleteAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(ForceMergeAction.NAME),
                ForceMergeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(ReadOnlyAction.NAME),
                ReadOnlyAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(RolloverAction.NAME),
                RolloverAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(ShrinkAction.NAME),
                ShrinkAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(WaitForSnapshotAction.NAME),
                WaitForSnapshotAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(FreezeAction.NAME),
                FreezeAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(SetPriorityAction.NAME),
                SetPriorityAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(MigrateAction.NAME),
                MigrateAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(SearchableSnapshotAction.NAME),
                SearchableSnapshotAction::parse),
            new NamedXContentRegistry.Entry(LifecycleAction.class,
                new ParseField(UnfollowAction.NAME),
                UnfollowAction::parse)
        );
    }
}
