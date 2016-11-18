/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.GetListAction.Response;
import org.elasticsearch.xpack.prelert.lists.ListDocument;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;
import org.elasticsearch.xpack.prelert.utils.SingleDocument;

import java.util.Collections;

public class GetListActionResponseTests extends AbstractStreamableTestCase<GetListAction.Response> {

    @Override
    protected Response createTestInstance() {
        final SingleDocument<ListDocument> result;
        if (randomBoolean()) {
            result = SingleDocument.empty(ListDocument.TYPE.getPreferredName());
        } else {
            result = new SingleDocument<>(ListDocument.TYPE.getPreferredName(),
                    new ListDocument(randomAsciiOfLengthBetween(1, 20), Collections.singletonList(randomAsciiOfLengthBetween(1, 20))));
        }
        return new Response(result);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
