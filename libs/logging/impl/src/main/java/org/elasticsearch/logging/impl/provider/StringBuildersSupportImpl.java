/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logging.impl.provider;

import org.apache.logging.log4j.util.StringBuilders;
import org.elasticsearch.logging.spi.StringBuildersSupport;

public class StringBuildersSupportImpl implements StringBuildersSupport {
    @Override
    public void escapeJsonImpl(StringBuilder toAppendTo, int start) {
        StringBuilders.escapeJson(toAppendTo, start);
    }
}
