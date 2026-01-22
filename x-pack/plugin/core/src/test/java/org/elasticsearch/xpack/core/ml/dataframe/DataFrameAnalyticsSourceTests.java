/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.QueryProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DataFrameAnalyticsSourceTests extends AbstractBWCSerializationTestCase<DataFrameAnalyticsSource> {

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
    protected DataFrameAnalyticsSource doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsSource.createParser(false).apply(parser, null);
    }

    @Override
    protected DataFrameAnalyticsSource createTestInstance() {
        return createRandom();
    }

    @Override
    protected DataFrameAnalyticsSource mutateInstance(DataFrameAnalyticsSource instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static DataFrameAnalyticsSource createRandom() {
        String[] index = generateRandomStringArray(10, 10, false, false);
        QueryProvider queryProvider = null;
        FetchSourceContext sourceFiltering = null;
        if (randomBoolean()) {
            try {
                queryProvider = QueryProvider.fromParsedQuery(QueryBuilders.termQuery(randomAlphaOfLength(10), randomAlphaOfLength(10)));
            } catch (IOException e) {
                // Should never happen
                throw new UncheckedIOException(e);
            }
        }
        if (randomBoolean()) {
            sourceFiltering = FetchSourceContext.of(
                true,
                generateRandomStringArray(10, 10, false, false),
                generateRandomStringArray(10, 10, false, false)
            );
        }
        Map<String, Object> runtimeMappings = null;
        if (randomBoolean()) {
            runtimeMappings = new HashMap<>();
            Map<String, Object> runtimeField = new HashMap<>();
            runtimeField.put("type", "keyword");
            runtimeField.put("script", "");
            runtimeMappings.put(randomAlphaOfLength(10), runtimeField);

        }
        return new DataFrameAnalyticsSource(index, queryProvider, sourceFiltering, runtimeMappings);
    }

    public static DataFrameAnalyticsSource mutateForVersion(DataFrameAnalyticsSource instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsSource> instanceReader() {
        return DataFrameAnalyticsSource::new;
    }

    public void testConstructor_GivenDisabledSource() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DataFrameAnalyticsSource(new String[] { "index" }, null, FetchSourceContext.DO_NOT_FETCH_SOURCE, null)
        );
        assertThat(e.getMessage(), equalTo("source._source cannot be disabled"));
    }

    public void testConstructor_GivenInvalidRuntimeMappings() {
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> new DataFrameAnalyticsSource(
                new String[] { "index" },
                null,
                null,
                Collections.singletonMap("typeless", Collections.singletonMap("not a type", "42"))
            )
        );
        assertThat(e.getMessage(), equalTo("No type specified for runtime field [typeless]"));
    }

    public void testIsFieldExcluded_GivenNoSourceFiltering() {
        DataFrameAnalyticsSource source = new DataFrameAnalyticsSource(new String[] { "index" }, null, null, null);
        assertThat(source.isFieldExcluded(randomAlphaOfLength(10)), is(false));
    }

    public void testIsFieldExcluded_GivenSourceFilteringWithNulls() {
        DataFrameAnalyticsSource source = new DataFrameAnalyticsSource(
            new String[] { "index" },
            null,
            FetchSourceContext.FETCH_SOURCE,
            null
        );
        assertThat(source.isFieldExcluded(randomAlphaOfLength(10)), is(false));
    }

    public void testIsFieldExcluded_GivenExcludes() {
        assertThat(newSourceWithExcludes("foo").isFieldExcluded("bar"), is(false));
        assertThat(newSourceWithExcludes("foo").isFieldExcluded("foo"), is(true));
        assertThat(newSourceWithExcludes("foo").isFieldExcluded("foo.bar"), is(true));
        assertThat(newSourceWithExcludes("foo*").isFieldExcluded("foo"), is(true));
        assertThat(newSourceWithExcludes("foo*").isFieldExcluded("foobar"), is(true));
        assertThat(newSourceWithExcludes("foo*").isFieldExcluded("foo.bar"), is(true));
        assertThat(newSourceWithExcludes("foo*").isFieldExcluded("foo*"), is(true));
        assertThat(newSourceWithExcludes("foo*").isFieldExcluded("fo*"), is(false));
    }

    public void testIsFieldExcluded_GivenIncludes() {
        assertThat(newSourceWithIncludes("foo").isFieldExcluded("bar"), is(true));
        assertThat(newSourceWithIncludes("foo").isFieldExcluded("foo"), is(false));
        assertThat(newSourceWithIncludes("foo").isFieldExcluded("foo.bar"), is(false));
        assertThat(newSourceWithIncludes("foo*").isFieldExcluded("foo"), is(false));
        assertThat(newSourceWithIncludes("foo*").isFieldExcluded("foobar"), is(false));
        assertThat(newSourceWithIncludes("foo*").isFieldExcluded("foo.bar"), is(false));
        assertThat(newSourceWithIncludes("foo*").isFieldExcluded("foo*"), is(false));
        assertThat(newSourceWithIncludes("foo*").isFieldExcluded("fo*"), is(true));
    }

    public void testIsFieldExcluded_GivenIncludesAndExcludes() {
        // Excludes take precedence
        assertThat(
            newSourceWithIncludesExcludes(Collections.singletonList("foo"), Collections.singletonList("foo")).isFieldExcluded("foo"),
            is(true)
        );
    }

    private static DataFrameAnalyticsSource newSourceWithIncludes(String... includes) {
        return newSourceWithIncludesExcludes(Arrays.asList(includes), Collections.emptyList());
    }

    private static DataFrameAnalyticsSource newSourceWithExcludes(String... excludes) {
        return newSourceWithIncludesExcludes(Collections.emptyList(), Arrays.asList(excludes));
    }

    private static DataFrameAnalyticsSource newSourceWithIncludesExcludes(List<String> includes, List<String> excludes) {
        FetchSourceContext sourceFiltering = FetchSourceContext.of(true, includes.toArray(new String[0]), excludes.toArray(new String[0]));
        return new DataFrameAnalyticsSource(new String[] { "index" }, null, sourceFiltering, null);
    }

    @Override
    protected DataFrameAnalyticsSource mutateInstanceForVersion(DataFrameAnalyticsSource instance, TransportVersion version) {
        return mutateForVersion(instance, version);
    }
}
