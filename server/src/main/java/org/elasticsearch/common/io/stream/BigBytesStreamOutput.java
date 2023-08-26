/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.io.InputStream;

public class BigBytesStreamOutput extends BytesStreamOutput implements OutputToInputStreamConvertable{

    public BigBytesStreamOutput(){
        this(0);
    }
    public BigBytesStreamOutput(int size){
        super(Math.max(size, PageCacheRecycler.PAGE_SIZE_IN_BYTES + 1), new BigArrays(null, null, null));
    }
    @Override
    public InputStream resetAndGetInputStream(){
        ByteArray byteArray = this.bytes;
        int count = this.count;
        this.bytes = bigArrays.newByteArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES + 1, false);
        this.count = 0;
        return new BigBytesStreamInput(byteArray, count);
    }
}
