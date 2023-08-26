/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import java.io.InputStream;
/**
 * OutputToInputStreamConvertable is transformation all or part of data in the current object to InputStream and reset current object.
 * on {@link StreamInput}.
 */
public interface OutputToInputStreamConvertable {


    /**
     * reset the current OutputStream and return InputStream.
    */

    InputStream resetAndGetInputStream();
}
