/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.List;

public class ResolvedIndexSerializingTests extends AbstractWireSerializingTestCase<ResolveIndexAction.ResolvedIndex> {

    @Override
    protected Writeable.Reader<ResolveIndexAction.ResolvedIndex> instanceReader() {
        return ResolveIndexAction.ResolvedIndex::new;
    }

    @Override
    protected ResolveIndexAction.ResolvedIndex mutateInstance(ResolveIndexAction.ResolvedIndex instance) {
        String name = instance.getName();
        String[] aliases = instance.getAliases();
        String[] attributes = instance.getAttributes();
        String dataStream = instance.getDataStream();
        IndexMode mode = instance.getMode();
        mode = randomValueOtherThan(mode, () -> randomFrom(IndexMode.values()));
        return new ResolveIndexAction.ResolvedIndex(name, aliases, attributes, dataStream, mode);
    }

    @Override
    protected ResolveIndexAction.ResolvedIndex createTestInstance() {
        return createTestItem();
    }

    private static ResolveIndexAction.ResolvedIndex createTestItem() {
        // Random index name
        final String name = randomAlphaOfLengthBetween(5, 20);

        // Random aliases (possibly empty)
        final String[] aliases = randomBoolean()
            ? new String[0]
            : randomArray(0, 4, String[]::new, () -> randomAlphaOfLengthBetween(3, 15));

        // Attributes: always one of "open"/"closed", plus optional flags
        final List<String> attrs = new ArrayList<>();
        attrs.add(randomBoolean() ? "open" : "closed");
        if (randomBoolean()) attrs.add("hidden");
        if (randomBoolean()) attrs.add("system");
        if (randomBoolean()) attrs.add("frozen");
        final String[] attributes = attrs.toArray(new String[0]);

        final String dataStream = randomBoolean() ? randomAlphaOfLengthBetween(3, 15) : null;

        final IndexMode mode = randomFrom(IndexMode.values());

        return new ResolveIndexAction.ResolvedIndex(name, aliases, attributes, dataStream, mode);

    }
}
