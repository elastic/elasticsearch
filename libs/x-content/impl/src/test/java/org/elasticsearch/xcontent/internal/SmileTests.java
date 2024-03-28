/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

public class SmileTests extends ESTestCase {

    public void testSmileBinaryContent() throws IOException {
        {
            BytesStreamOutput bos = new BytesStreamOutput();
            XContentBuilder builder = XContentFactory.smileBuilder(bos).startObject().endObject();
            builder.close();
            BytesReference ref = bos.bytes();
            assertTrue(XContentType.SMILE.xContent().detectContent(ref.array(), ref.arrayOffset(), ref.length()));
            assertFalse(XContentType.STREAM_SMILE.xContent().detectContent(ref.array(), ref.arrayOffset(), ref.length()));
        }
        {
            BytesStreamOutput bos = new BytesStreamOutput();
            XContentBuilder builder = XContentFactory.streamSmileBuilder(bos).startObject().endObject();
            builder.close();
            BytesReference ref = bos.bytes();
            assertTrue(XContentType.STREAM_SMILE.xContent().detectContent(ref.array(), ref.arrayOffset(), ref.length()));
            assertFalse(XContentType.SMILE.xContent().detectContent(ref.array(), ref.arrayOffset(), ref.length()));
        }
    }
}
