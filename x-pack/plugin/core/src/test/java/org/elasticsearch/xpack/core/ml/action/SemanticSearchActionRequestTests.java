/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigUpdateTests;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class SemanticSearchActionRequestTests extends AbstractWireSerializingTestCase<SemanticSearchAction.Request> {
    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerNamedXContents() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        var xContents = new ArrayList<>(searchModule.getNamedXContents());
        xContents.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContentRegistry = new NamedXContentRegistry(xContents);

        var writeables = new ArrayList<>(searchModule.getNamedWriteables());
        writeables.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(writeables);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected Writeable.Reader<SemanticSearchAction.Request> instanceReader() {
        return SemanticSearchAction.Request::new;
    }

    @Override
    protected SemanticSearchAction.Request createTestInstance() {
        return new SemanticSearchAction.Request(
            generateRandomStringArray(1, 5, false),
            randomBoolean() ? null : randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            SemanticSearchActionKnnQueryOptionsTests.randomInstance(),
            TextEmbeddingConfigUpdateTests.randomUpdate(),
            randomBoolean() ? null : TimeValue.timeValueSeconds(randomIntBetween(1, 10)),
            randomBoolean() ? null : List.of(new TermsQueryBuilder("foo", "bar", "cat")),
            randomBoolean() ? null : FetchSourceContext.of(randomBoolean()),
            randomBoolean() ? null : List.of(new FieldAndFormat("foo", null)),
            randomBoolean() ? null : List.of(new FieldAndFormat("foo", null)),
            randomBoolean() ? null : StoredFieldsContext.fromList(List.of("A", "B"))
        );
    }

    public void testValidate() {
        var validAction = createTestInstance();
        assertNull(validAction.validate());

        var action = new SemanticSearchAction.Request(
            new String[] { "foo" },
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        var validation = action.validate();
        assertNotNull(validation);
        assertThat(validation.validationErrors(), hasSize(3));
        assertThat(validation.validationErrors().get(0), containsString("query_string cannot be null"));
        assertThat(validation.validationErrors().get(1), containsString("model_id cannot be null"));
        assertThat(validation.validationErrors().get(2), containsString("knn cannot be null"));
    }
}
