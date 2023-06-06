/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.synonyms.SynonymsManagementAPIService.UpdateSynonymsResult.CREATED;
import static org.elasticsearch.synonyms.SynonymsManagementAPIService.UpdateSynonymsResult.UPDATED;

public class PutSynonymsActionResponseSerializingTests extends AbstractWireSerializingTestCase<PutSynonymsAction.Response> {

    @Override
    protected Writeable.Reader<PutSynonymsAction.Response> instanceReader() {
        return PutSynonymsAction.Response::new;
    }

    @Override
    protected PutSynonymsAction.Response createTestInstance() {
        return new PutSynonymsAction.Response(randomBoolean() ? CREATED : UPDATED);
    }

    @Override
    protected PutSynonymsAction.Response mutateInstance(PutSynonymsAction.Response instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
