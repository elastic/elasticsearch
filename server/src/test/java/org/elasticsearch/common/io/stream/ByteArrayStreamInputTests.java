/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;

public class ByteArrayStreamInputTests extends AbstractStreamTests {
    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) {
        final BytesRef bytesRef = bytesReference.toBytesRef();
        final ByteArrayStreamInput byteArrayStreamInput = new ByteArrayStreamInput();
        byteArrayStreamInput.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        return byteArrayStreamInput;
    }
}
