/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.Request;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Arrays;
import java.util.EnumSet;

public class ResolveIndexRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        String[] names = generateRandomStringArray(1, 4, false);
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        if (randomBoolean()) {
            return new Request(names, indicesOptions);
        } else {
            EnumSet<IndexMode> randomModes = EnumSet.copyOf(randomNonEmptySubsetOf(Arrays.asList(IndexMode.values())));
            return new Request(names, indicesOptions, randomBoolean() ? null : randomModes);
        }
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
