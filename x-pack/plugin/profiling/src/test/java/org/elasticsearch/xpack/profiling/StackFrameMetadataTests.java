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

public class StackFrameMetadataTests extends ESTestCase {
    public void testToXContent() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
            .field("FrameID", "VZbhUAtQEfdUptXfb8HvNgAAAAAAsbAE")
            .field("FileID", "6tVKI4mSYDEJ-ABAIpYXcg")
            .field("FrameType", 1)
            .field("Inline", false)
            .field("AddressOrLine", 23)
            .field("FunctionName", "PyDict_GetItemWithError")
            .field("FunctionOffset", 2567)
            .field("SourceFilename", "/build/python3.9-RNBry6/python3.9-3.9.2/Objects/dictobject.c")
            .field("SourceLine", 1456)
            .field("ExeFileName", "python3.9")
            .endObject();

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        StackFrameMetadata stackTraceMetadata = new StackFrameMetadata(
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
        stackTraceMetadata.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

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
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            metadata,
            (o -> new StackFrameMetadata(
                o.frameID,
                o.fileID,
                o.frameType,
                o.inline,
                o.addressOrLine,
                o.functionName,
                o.functionOffset,
                o.sourceFilename,
                o.sourceLine,
                o.exeFilename
            ))
        );
    }
}
