/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.cbor;

import org.elasticsearch.common.xcontent.BaseXContentTestCase;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.cbor.CborXContent;

import java.io.ByteArrayOutputStream;

public class CborXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.CBOR;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        XContentGenerator generator = CborXContent.cborXContent.createGenerator(os);
        doTestBigInteger(generator, os);
    }

    public void testAllowsDuplicates() throws Exception {
        try (XContentParser xParser = createParser(builder().startObject().endObject())) {
            expectThrows(UnsupportedOperationException.class, () -> xParser.allowDuplicateKeys(true));
        }
    }
}
