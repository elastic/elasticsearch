/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.bytes.BytesReference;

public class ESMessageHelper {

    public static boolean isPlaintextElasticsearchMessage(BytesReference reference) {
        return reference.get(0) == 'E' && reference.get(1) == 'S';
    }
}
