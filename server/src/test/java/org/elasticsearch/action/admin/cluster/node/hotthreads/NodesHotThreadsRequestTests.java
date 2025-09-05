/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NodesHotThreadsRequestTests extends ESTestCase {

    public void testBWCSerialization() throws IOException {
        TimeValue sampleInterval = new TimeValue(50, TimeUnit.MINUTES);

        NodesHotThreadsRequest request = new NodesHotThreadsRequest(
            new String[] { "123" },
            new HotThreads.RequestOptions(
                4,
                HotThreads.ReportType.BLOCK,
                HotThreads.RequestOptions.DEFAULT.sortOrder(),
                sampleInterval,
                3,
                false
            )
        );

        TransportVersion latest = TransportVersion.current();
        TransportVersion previous = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersionUtils.getFirstVersion(),
            TransportVersionUtils.getPreviousVersion(TransportVersion.current())
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(latest);
            request.requestOptions.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(previous);
                NodesHotThreadsRequest deserialized = new NodesHotThreadsRequest(
                    Strings.EMPTY_ARRAY,
                    HotThreads.RequestOptions.readFrom(in)
                );
                assertEquals(request.threads(), deserialized.threads());
                assertEquals(request.ignoreIdleThreads(), deserialized.ignoreIdleThreads());
                assertEquals(request.type(), deserialized.type());
                assertEquals(request.interval(), deserialized.interval());
                assertEquals(request.snapshots(), deserialized.snapshots());

            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(previous);
            request.requestOptions.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(previous);
                NodesHotThreadsRequest deserialized = new NodesHotThreadsRequest(
                    Strings.EMPTY_ARRAY,
                    HotThreads.RequestOptions.readFrom(in)
                );
                assertEquals(request.threads(), deserialized.threads());
                assertEquals(request.ignoreIdleThreads(), deserialized.ignoreIdleThreads());
                assertEquals(request.type(), deserialized.type());
                assertEquals(request.interval(), deserialized.interval());
                assertEquals(request.snapshots(), deserialized.snapshots());
            }
        }
    }
}
