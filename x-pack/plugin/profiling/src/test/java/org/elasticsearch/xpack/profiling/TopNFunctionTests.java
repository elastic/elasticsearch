/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class TopNFunctionTests extends ESTestCase {
    public void testToXContent() throws IOException {
        StackFrameMetadata metadata = new StackFrameMetadata(
            "VZbhUAtQEfdUptXfb8HvNgAAAAAAsbAE",
            "6tVKI4mSYDEJ-ABAIpYXcg",
            1,
            false,
            23,
            "PyDict_GetItemWithError",
            2567,
            "/build/python3.9-RNBry6/python3.9-3.9.2/Objects/dictobject.c",
            1456,
            "python3.9"
        );
        String frameGroupID = FrameGroupID.create(
            metadata.fileID,
            metadata.addressOrLine,
            metadata.exeFilename,
            metadata.sourceFilename,
            metadata.functionName
        );

        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
            .field("Id", frameGroupID)
            .field("Rank", 1)
            .field("Frame", metadata)
            .field("CountExclusive", 1)
            .field("CountInclusive", 10)
            .endObject();

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        TopNFunction topNFunction = new TopNFunction(frameGroupID.toString(), 1, metadata, 1, 10);
        topNFunction.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedRequest), BytesReference.bytes(actualRequest), contentType);
    }

    public void testEquality() {
        StackFrameMetadata metadata = new StackFrameMetadata(
            "VZbhUAtQEfdUptXfb8HvNgAAAAAAsbAE",
            "6tVKI4mSYDEJ-ABAIpYXcg",
            1,
            false,
            23,
            "PyDict_GetItemWithError",
            2567,
            "/build/python3.9-RNBry6/python3.9-3.9.2/Objects/dictobject.c",
            1456,
            "python3.9"
        );
        String frameGroupID = FrameGroupID.create(
            metadata.fileID,
            metadata.addressOrLine,
            metadata.exeFilename,
            metadata.sourceFilename,
            metadata.functionName
        );
        TopNFunction topNFunction = new TopNFunction(frameGroupID, 1, metadata, 1, 10);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            topNFunction,
            (o -> new TopNFunction(o.id, o.rank, o.metadata, o.exclusiveCount, o.inclusiveCount))
        );
    }
}
