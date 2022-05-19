/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LookupFieldTests extends AbstractWireSerializingTestCase<LookupField> {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry xContentRegistry;

    @Before
    public void setupXContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(IndicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected Writeable.Reader<LookupField> instanceReader() {
        return LookupField::new;
    }

    public static LookupField randomInstance() {
        final String lookupIndex = randomAlphaOfLength(10);
        final QueryBuilder queryBuilder = QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20));
        return new LookupField(lookupIndex, queryBuilder, randomFetchFields(), randomIntBetween(1, 10));
    }

    private static List<FieldAndFormat> randomFetchFields() {
        final int numFetchFields = randomIntBetween(1, 10);
        final List<FieldAndFormat> fetchFields = new ArrayList<>();
        for (int i = 0; i < numFetchFields; i++) {
            String format = randomBoolean() ? randomAlphaOfLength(5) : null;
            String field = randomAlphaOfLength(10);
            fetchFields.add(new FieldAndFormat(field, format));
        }
        return fetchFields;
    }

    @Override
    protected LookupField mutateInstance(LookupField old) throws IOException {
        String lookupIndex = old.targetIndex();
        QueryBuilder query = old.query();
        List<FieldAndFormat> fetchFields = old.fetchFields();
        int size = old.size();
        int iterations = iterations(1, 5);
        for (int i = 0; i < iterations; i++) {
            switch (randomIntBetween(0, 3)) {
                case 0 -> lookupIndex = randomValueOtherThan(old.targetIndex(), () -> randomAlphaOfLength(10));
                case 1 -> query = randomValueOtherThan(
                    old.query(),
                    () -> QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20))
                );
                case 2 -> fetchFields = randomValueOtherThan(old.fetchFields(), LookupFieldTests::randomFetchFields);
                case 3 -> size = randomValueOtherThan(old.size(), () -> randomIntBetween(1, 10));
                default -> throw new AssertionError();
            }
        }
        return new LookupField(lookupIndex, query, fetchFields, size);
    }

    @Override
    protected LookupField createTestInstance() {
        return randomInstance();
    }

    public void testToSearchRequest() {
        LookupField lookupField = createTestInstance();
        final SearchRequest localRequest = lookupField.toSearchRequest(randomBoolean() ? null : "");
        assertThat(localRequest.source().query(), equalTo(lookupField.query()));
        assertThat(localRequest.source().size(), equalTo(lookupField.size()));
        assertThat(localRequest.indices(), equalTo(new String[] { lookupField.targetIndex() }));
        assertThat(localRequest.source().fetchFields(), equalTo(lookupField.fetchFields()));

        final String clusterAlias = randomAlphaOfLength(10);
        final SearchRequest remoteRequest = lookupField.toSearchRequest(clusterAlias);
        assertThat(remoteRequest.source().query(), equalTo(lookupField.query()));
        assertThat(remoteRequest.source().size(), equalTo(lookupField.size()));
        assertThat(remoteRequest.indices(), equalTo(new String[] { clusterAlias + ":" + lookupField.targetIndex() }));
        assertThat(remoteRequest.source().fetchFields(), equalTo(lookupField.fetchFields()));
    }
}
