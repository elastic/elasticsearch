/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.synonyms.PagedResult;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSetSummary;

public class GetSynonymsSetsActionResponseSerializingTests extends AbstractWireSerializingTestCase<GetSynonymsSetsAction.Response> {

    @Override
    protected Writeable.Reader<GetSynonymsSetsAction.Response> instanceReader() {
        return GetSynonymsSetsAction.Response::new;
    }

    @Override
    protected GetSynonymsSetsAction.Response createTestInstance() {
        return new GetSynonymsSetsAction.Response(new PagedResult<>(randomLongBetween(0, Long.MAX_VALUE), randomSynonymsSetSummary()));
    }

    @Override
    protected GetSynonymsSetsAction.Response mutateInstance(GetSynonymsSetsAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
