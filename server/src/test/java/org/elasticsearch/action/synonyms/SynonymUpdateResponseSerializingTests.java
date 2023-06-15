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

public class SynonymUpdateResponseSerializingTests extends AbstractWireSerializingTestCase<SynonymUpdateResponse> {

    @Override
    protected Writeable.Reader<SynonymUpdateResponse> instanceReader() {
        return SynonymUpdateResponse::new;
    }

    @Override
    protected SynonymUpdateResponse createTestInstance() {
        return new SynonymUpdateResponse(randomBoolean() ? CREATED : UPDATED);
    }

    @Override
    protected SynonymUpdateResponse mutateInstance(SynonymUpdateResponse instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
