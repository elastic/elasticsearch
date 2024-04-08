/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent.provider;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.TSFBuilder;

public class XContentImplUtils {
    public static <F extends JsonFactory, B extends TSFBuilder<F, B>> F configure(TSFBuilder<F, B> builder) {
        // jackson 2.15 introduced a max string length. We have other limits in place to constrain max doc size,
        // so here we set to max value (2GiB) so as not to constrain further than those existing limits.
        return builder.streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build()).build();
    }
}
