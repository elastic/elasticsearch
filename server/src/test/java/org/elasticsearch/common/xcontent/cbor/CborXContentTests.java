/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.cbor;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;

import org.elasticsearch.common.xcontent.BaseXContentTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.ByteArrayOutputStream;

public class CborXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.CBOR;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        JsonGenerator generator = new CBORFactory().createGenerator(os);
        doTestBigInteger(generator, os);
    }

    public void testAllowsDuplicates() throws Exception {
        try (XContentParser xParser = createParser(builder().startObject().endObject())) {
            expectThrows(UnsupportedOperationException.class, () -> xParser.allowDuplicateKeys(true));
        }
    }
}
