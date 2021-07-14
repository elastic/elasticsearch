/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.persistent.CompletionPersistentTaskAction.Request;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompletionPersistentTaskRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        if (randomBoolean()) {
            return new Request(randomAlphaOfLength(10), randomNonNegativeLong(), null, null);
        } else {
            return new Request(randomAlphaOfLength(10), randomNonNegativeLong(), null, randomAlphaOfLength(20));
        }
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testSerializeToOldNodeThrows() {
        Request request = new Request(randomAlphaOfLength(10), randomNonNegativeLong(), null, randomAlphaOfLength(20));
        StreamOutput out = mock(StreamOutput.class);
        when(out.getVersion()).thenReturn(Version.V_7_14_0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> request.writeTo(out));
        assertThat(e.getMessage(), equalTo("attempt to abort a persistent task locally in a cluster that contains a node that is too "
            + "old: found node version [7.14.0], minimum required [7.15.0]"));
    }
}
