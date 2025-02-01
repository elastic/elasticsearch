/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

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
        String fileID = "6tVKI4mSYDEJ-ABAIpYXcg";
        int frameType = 1;
        boolean inline = false;
        int addressOrLine = 23;
        String functionName = "PyDict_GetItemWithError";
        String sourceFilename = "/build/python3.9-RNBry6/python3.9-3.9.2/Objects/dictobject.c";
        int sourceLine = 1456;
        String exeFilename = "python3.9";

        String frameGroupID = FrameGroupID.create(fileID, addressOrLine, exeFilename, sourceFilename, functionName);

        XContentType contentType = randomFrom(XContentType.values());
        // tag::noformat
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
                .field("id", frameGroupID)
                .field("rank", 1)
                .startObject("frame")
                    .field("frame_type", frameType)
                    .field("inline", inline)
                    .field("address_or_line", addressOrLine)
                    .field("function_name", functionName)
                    .field("file_name", sourceFilename)
                    .field("line_number", sourceLine)
                    .field("executable_file_name", exeFilename)
                .endObject()
                .startObject("sub_groups")
                    .startObject("transaction.name")
                        .startObject("basket")
                            .field("count", 7L)
                        .endObject()
                    .endObject()
                .endObject()
                .field("self_count", 1)
                .field("total_count", 10)
                .field("self_annual_co2_tons").rawValue("2.2000")
                .field("total_annual_co2_tons").rawValue("22.0000")
                .field("self_annual_costs_usd").rawValue("12.0000")
                .field("total_annual_costs_usd").rawValue("120.0000")
            .endObject();
        // end::noformat

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        TopNFunction topNFunction = new TopNFunction(
            frameGroupID,
            1,
            frameType,
            inline,
            addressOrLine,
            functionName,
            sourceFilename,
            sourceLine,
            exeFilename,
            1,
            10,
            2.2d,
            22.0d,
            12.0d,
            120.0d,
            SubGroup.root("transaction.name").addCount("basket", 7L)
        );
        topNFunction.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedRequest), BytesReference.bytes(actualRequest), contentType);
    }

    public void testEquality() {
        String fileID = "6tVKI4mSYDEJ-ABAIpYXcg";
        int frameType = 1;
        boolean inline = false;
        int addressOrLine = 23;
        String functionName = "PyDict_GetItemWithError";
        String sourceFilename = "/build/python3.9-RNBry6/python3.9-3.9.2/Objects/dictobject.c";
        int sourceLine = 1456;
        String exeFilename = "python3.9";

        String frameGroupID = FrameGroupID.create(fileID, addressOrLine, exeFilename, sourceFilename, functionName);

        TopNFunction topNFunction = new TopNFunction(
            frameGroupID,
            1,
            frameType,
            inline,
            addressOrLine,
            functionName,
            sourceFilename,
            sourceLine,
            exeFilename,
            1,
            10,
            2.0d,
            4.0d,
            23.2d,
            12.0d,
            SubGroup.root("transaction.name").addCount("checkout", 4L).addCount("basket", 12L)
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(topNFunction, (TopNFunction::copy));
    }
}
