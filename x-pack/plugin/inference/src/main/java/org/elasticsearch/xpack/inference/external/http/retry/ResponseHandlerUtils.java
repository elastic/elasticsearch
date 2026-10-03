/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.MessageSupport;
import org.elasticsearch.common.Strings;

public class ResponseHandlerUtils {
    public static String getFirstHeaderOrUnknown(HttpResponse response, String name) {
        var header = response.getFirstHeader(name);
        if (header != null && Strings.isNullOrEmpty(header.getValue()) == false) {
            // MessageSupport.parse reproduces the 4.x Header#getElements() semantics (quoting, name=value elements),
            // so this keeps returning the first parsed element's name (e.g. model ids)
            var elements = MessageSupport.parse(header);
            if (elements.length > 0 && Strings.isNullOrEmpty(elements[0].getName()) == false) {
                return elements[0].getName();
            }
        }
        return "unknown";
    }

    private ResponseHandlerUtils() {}
}
