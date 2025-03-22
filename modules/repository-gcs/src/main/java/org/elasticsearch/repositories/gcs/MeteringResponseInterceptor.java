/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;

final class MeteringResponseInterceptor implements HttpResponseInterceptor {

    @Override
    public void interceptResponse(final HttpResponse response) {
        var stats = OperationStats.get();
        var code = response.getStatusCode();
        stats.reqAtt += 1;
        if (((code >= 200 && code < 300) || code == 308) == false) {
            stats.reqErr += 1;
            if (code >= 400 && code < 500) {
                stats.reqBillableErr += 1;
            }
            switch (code) {
                case 416 -> stats.reqErrRange += 1;
                case 429 -> stats.reqErrThrottle += 1;
            }
        }
    }
}
