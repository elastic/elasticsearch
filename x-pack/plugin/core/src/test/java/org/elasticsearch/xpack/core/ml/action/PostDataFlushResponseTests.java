/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction.Response;
import org.joda.time.DateTime;

public class PostDataFlushResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(randomBoolean(), new DateTime(randomDateTimeZone()).toDate());
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }
}