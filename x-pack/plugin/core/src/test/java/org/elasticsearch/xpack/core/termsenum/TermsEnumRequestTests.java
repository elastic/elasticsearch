/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.termsenum;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumAction;
import org.elasticsearch.xpack.core.termsenum.action.TermsEnumRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class TermsEnumRequestTests extends AbstractSerializingTestCase<TermsEnumRequest> {
    private NamedXContentRegistry xContentRegistry;
    private NamedWriteableRegistry namedWriteableRegistry;

    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected TermsEnumRequest createTestInstance() {
        TermsEnumRequest request = new TermsEnumRequest();
        request.size(randomIntBetween(1, 20));
        request.field(randomAlphaOfLengthBetween(3, 10));
        request.caseInsensitive(randomBoolean());
        if (randomBoolean()) {
            request.indexFilter(QueryBuilders.termQuery("field", randomAlphaOfLength(5)));
        }
        String[] randomIndices = new String[randomIntBetween(1, 5)];
        for (int i = 0; i < randomIndices.length; i++) {
            randomIndices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        request.indices(randomIndices);
        if (randomBoolean()) {
            request.indicesOptions(randomBoolean() ? IndicesOptions.strictExpand() : IndicesOptions.lenientExpandOpen());
        }
        return request;
    }

    @Override
    protected TermsEnumRequest createXContextTestInstance(XContentType xContentType) {
        return createTestInstance()
            // these options are outside of the xcontent
            .indices("test")
            .indicesOptions(TermsEnumRequest.DEFAULT_INDICES_OPTIONS);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    @Override
    protected Writeable.Reader<TermsEnumRequest> instanceReader() {
        return TermsEnumRequest::new;
    }

    @Override
    protected TermsEnumRequest doParseInstance(XContentParser parser) throws IOException {
        return TermsEnumAction.fromXContent(parser, "test");
    }

    @Override
    protected TermsEnumRequest mutateInstance(TermsEnumRequest instance) throws IOException {
        List<Consumer<TermsEnumRequest>> mutators = new ArrayList<>();
        mutators.add(request -> { request.field(randomValueOtherThan(request.field(), () -> randomAlphaOfLengthBetween(3, 10))); });
        mutators.add(request -> {
            String[] indices = ArrayUtils.concat(instance.indices(), generateRandomStringArray(5, 10, false, false));
            request.indices(indices);
        });
        mutators.add(request -> {
            IndicesOptions indicesOptions = randomValueOtherThan(
                request.indicesOptions(),
                () -> IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
            request.indicesOptions(indicesOptions);
        });
        mutators.add(
            request -> request.indexFilter(request.indexFilter() != null ? request.indexFilter().boost(2) : QueryBuilders.matchAllQuery())
        );
        TermsEnumRequest mutatedInstance = copyInstance(instance);
        Consumer<TermsEnumRequest> mutator = randomFrom(mutators);
        mutator.accept(mutatedInstance);
        return mutatedInstance;
    }
}
