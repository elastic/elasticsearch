/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.http.auth;

import org.elasticsearch.common.xcontent.ToXContentObject;

public interface HttpAuth extends ToXContentObject {

    String type();

}
