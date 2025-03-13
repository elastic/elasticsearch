/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class ResolveClusterActionRequestTests extends AbstractWireSerializingTestCase<ResolveClusterActionRequest> {

    @Override
    protected Writeable.Reader<ResolveClusterActionRequest> instanceReader() {
        return ResolveClusterActionRequest::new;
    }

    @Override
    protected ResolveClusterActionRequest createTestInstance() {
        if (randomInt(5) == 3) {
            return new ResolveClusterActionRequest(new String[0], IndicesOptions.DEFAULT, true, randomBoolean());
        } else {
            String[] names = generateRandomStringArray(1, 7, false);
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
            return new ResolveClusterActionRequest(names, indicesOptions, false, randomBoolean());
        }
    }

    @Override
    protected ResolveClusterActionRequest mutateInstance(ResolveClusterActionRequest instance) throws IOException {
        List<Consumer<ResolveClusterActionRequest>> mutators = new ArrayList<>();
        mutators.add(request -> {
            String[] names = ArrayUtils.concat(request.indices(), new String[] { randomAlphaOfLength(10) });
            request.indices(names);
        });
        mutators.add(request -> {
            String[] indices = ArrayUtils.concat(instance.indices(), generateRandomStringArray(5, 10, false, false));
            request.indices(indices);
        });
        mutators.add(request -> {
            IndicesOptions indicesOptions = randomValueOtherThan(
                request.indicesOptions(),
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
            request.indicesOptions(indicesOptions);
        });

        ResolveClusterActionRequest mutatedInstance = copyInstance(instance);
        Consumer<ResolveClusterActionRequest> mutator = randomFrom(mutators);
        mutator.accept(mutatedInstance);
        return mutatedInstance;
    }

    public void testLocalIndicesPresent() {
        {
            String[] indicesOrig = new String[] { "*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indicesOrig);
            request.indices("remote1:foo");
            assertThat(request.isLocalIndicesRequested(), equalTo(true));
        }
        {
            String[] indicesOrig = new String[] { "remote1:foo" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indicesOrig);
            assertThat(request.isLocalIndicesRequested(), equalTo(false));
        }
        {
            String[] indicesOrig = new String[] { "remote1:foo", "bar*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indicesOrig);
            assertThat(request.isLocalIndicesRequested(), equalTo(true));
        }
        {
            String[] indicesOrig = new String[] { "remote1:foo", "bar*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indicesOrig);
            request.indices("remote1:foo"); // simulate security layer removing the local indices
            assertThat(request.isLocalIndicesRequested(), equalTo(true));  // original request had it so should be true
        }
        {
            String[] indicesOrig = new String[] { "*:foo", "bar*", "-bar", "-remote1:*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indicesOrig);
            assertThat(request.isLocalIndicesRequested(), equalTo(true));
        }
        {
            String[] indicesOrig = new String[] { "*:foo", "-remote1:*" };
            ResolveClusterActionRequest request = new ResolveClusterActionRequest(indicesOrig);
            assertThat(request.isLocalIndicesRequested(), equalTo(false));
        }
    }
}
