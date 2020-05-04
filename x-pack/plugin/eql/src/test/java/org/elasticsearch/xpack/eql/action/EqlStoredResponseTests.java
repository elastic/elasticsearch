/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;

import static org.elasticsearch.xpack.eql.action.EqlSearchResponseTests.randomEqlSearchResponse;

public class EqlStoredResponseTests extends AbstractWireSerializingTestCase<EqlStoredResponse> {
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected EqlStoredResponse createTestInstance() {
        if (randomBoolean()) {
            return new EqlStoredResponse(new IllegalArgumentException(randomAlphaOfLength(10)), randomNonNegativeLong());
        } else {
            return new EqlStoredResponse(randomEqlSearchResponse(), randomNonNegativeLong());
        }
    }

    @Override
    protected Writeable.Reader<EqlStoredResponse> instanceReader() {
        return EqlStoredResponse::new;
    }

}
