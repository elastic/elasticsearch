/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ExplainLifecycleResponseTests extends AbstractXContentSerializingTestCase<ExplainLifecycleResponse> {

    @Override
    protected ExplainLifecycleResponse createTestInstance() {
        Map<String, IndexLifecycleExplainResponse> indexResponses = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 2); i++) {
            IndexLifecycleExplainResponse indexResponse = IndexLifecycleExplainResponseTests.randomIndexExplainResponse();
            indexResponses.put(indexResponse.getIndex(), indexResponse);
        }
        return new ExplainLifecycleResponse(indexResponses);
    }

    @Override
    protected Writeable.Reader<ExplainLifecycleResponse> instanceReader() {
        return ExplainLifecycleResponse::new;
    }

    @Override
    protected ExplainLifecycleResponse mutateInstance(ExplainLifecycleResponse response) {
        Map<String, IndexLifecycleExplainResponse> indexResponses = new HashMap<>(response.getIndexResponses());
        IndexLifecycleExplainResponse indexResponse = IndexLifecycleExplainResponseTests.randomIndexExplainResponse();
        indexResponses.put(indexResponse.getIndex(), indexResponse);
        return new ExplainLifecycleResponse(indexResponses);
    }

    @Override
    protected ExplainLifecycleResponse doParseInstance(XContentParser parser) throws IOException {
        return ExplainLifecycleResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new))
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            CollectionUtils.appendToCopy(
                ClusterModule.getNamedXWriteables(),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MockAction.NAME), MockAction::parse)
            )
        );
    }
}
