/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.util.List;

public class ItemUsageTests extends AbstractWireTestCase<ItemUsage> {

    public static ItemUsage randomUsage() {
        return new ItemUsage(randomStringList(), randomStringList(), randomStringList());
    }

    @Nullable
    private static List<String> randomStringList() {
        if (randomBoolean()) {
            return null;
        } else {
            return randomList(0, 1, () -> randomAlphaOfLengthBetween(2, 10));
        }
    }

    @Override
    protected ItemUsage createTestInstance() {
        return randomUsage();
    }

    @Override
    protected ItemUsage copyInstance(ItemUsage instance, Version version) throws IOException {
        return new ItemUsage(instance.getIndices(), instance.getDataStreams(), instance.getComposableTemplates());
    }

    @Override
    protected ItemUsage mutateInstance(ItemUsage instance) throws IOException {
        return super.mutateInstance(instance);
    }
}
