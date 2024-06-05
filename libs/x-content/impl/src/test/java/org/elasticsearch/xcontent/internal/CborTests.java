/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.internal;

import com.fasterxml.jackson.dataformat.cbor.CBORConstants;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import static org.hamcrest.Matchers.equalTo;

public class CborTests extends ESTestCase {

    public void testCBORBasedOnMajorObjectDetection() {
        // for this {"f "=> 5} perl encoder for example generates:
        byte[] bytes = new byte[] { (byte) 0xA1, (byte) 0x43, (byte) 0x66, (byte) 6f, (byte) 6f, (byte) 0x5 };
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.CBOR));

        // this if for {"foo" : 5} in python CBOR
        bytes = new byte[] { (byte) 0xA1, (byte) 0x63, (byte) 0x66, (byte) 0x6f, (byte) 0x6f, (byte) 0x5 };
        assertThat(XContentFactory.xContentType(bytes), equalTo(XContentType.CBOR));
        assertThat(((Number) XContentHelper.convertToMap(new BytesArray(bytes), true).v2().get("foo")).intValue(), equalTo(5));

        // also make sure major type check doesn't collide with SMILE and JSON, just in case
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, SmileConstants.HEADER_BYTE_1), equalTo(false));
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, (byte) '{'), equalTo(false));
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, (byte) ' '), equalTo(false));
        assertThat(CBORConstants.hasMajorType(CBORConstants.MAJOR_TYPE_OBJECT, (byte) '-'), equalTo(false));
    }
}
