/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.xcontent.spi;

import org.elasticsearch.xcontent.MediaType;

import java.util.List;
import java.util.Map;

public interface MediaTypeProvider {
    /** Extensions that implement their own concrete {@link MediaType}s provide them through this interface method */
    List<MediaType> getMediaTypes();

    /** Extensions that implement additional {@link MediaType} aliases provide them through this interface method */
    Map<String, MediaType> getAdditionalMediaTypes();
}
