/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.ClassificationTests;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.MlDataFrameAnalysisNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetectionTests;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RegressionTests;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class DataFrameAnalyticsConfigTests extends AbstractBWCSerializationTestCase<DataFrameAnalyticsConfig> {

    @Override
    protected DataFrameAnalyticsConfig doParseInstance(XContentParser parser) throws IOException {
        ObjectParser<DataFrameAnalyticsConfig.Builder, Void> dataFrameAnalyticsConfigParser = lenient
            ? DataFrameAnalyticsConfig.LENIENT_PARSER
            : DataFrameAnalyticsConfig.STRICT_PARSER;
        return dataFrameAnalyticsConfigParser.apply(parser, null).build();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
        return new NamedWriteableRegistry(namedWriteables);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected DataFrameAnalyticsConfig createTestInstance() {
        return createRandom(randomValidId(), lenient);
    }

    @Override
    protected DataFrameAnalyticsConfig mutateInstance(DataFrameAnalyticsConfig instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected DataFrameAnalyticsConfig mutateInstanceForVersion(DataFrameAnalyticsConfig instance, TransportVersion version) {
        DataFrameAnalyticsConfig.Builder builder = new DataFrameAnalyticsConfig.Builder(instance).setSource(
            DataFrameAnalyticsSourceTests.mutateForVersion(instance.getSource(), version)
        ).setDest(DataFrameAnalyticsDestTests.mutateForVersion(instance.getDest(), version));
        if (instance.getAnalysis() instanceof OutlierDetection) {
            builder.setAnalysis(OutlierDetectionTests.mutateForVersion((OutlierDetection) instance.getAnalysis(), version));
        }
        if (instance.getAnalysis() instanceof Regression) {
            builder.setAnalysis(RegressionTests.mutateForVersion((Regression) instance.getAnalysis(), version));
        }
        if (instance.getAnalysis() instanceof Classification) {
            builder.setAnalysis(ClassificationTests.mutateForVersion((Classification) instance.getAnalysis(), version));
        }
        if (version.before(TransportVersions.V_8_8_0)) {
            builder.setMeta(null);
        }
        return builder.build();
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsConfig> instanceReader() {
        return DataFrameAnalyticsConfig::new;
    }

    public static DataFrameAnalyticsConfig createRandom(String id) {
        return createRandom(id, false);
    }

    public static DataFrameAnalyticsConfig createRandom(String id, boolean withGeneratedFields) {
        return createRandomBuilder(
            id,
            withGeneratedFields,
            randomFrom(OutlierDetectionTests.createRandom(), RegressionTests.createRandom(), ClassificationTests.createRandom())
        ).build();
    }

    public static DataFrameAnalyticsConfig.Builder createRandomBuilder(String id) {
        return createRandomBuilder(
            id,
            false,
            randomFrom(OutlierDetectionTests.createRandom(), RegressionTests.createRandom(), ClassificationTests.createRandom())
        );
    }

    public static DataFrameAnalyticsConfig.Builder createRandomBuilder(String id, boolean withGeneratedFields, DataFrameAnalysis analysis) {
        DataFrameAnalyticsSource source = DataFrameAnalyticsSourceTests.createRandom();
        DataFrameAnalyticsDest dest = DataFrameAnalyticsDestTests.createRandom();
        DataFrameAnalyticsConfig.Builder builder = new DataFrameAnalyticsConfig.Builder().setId(id)
            .setAnalysis(analysis)
            .setSource(source)
            .setDest(dest);
        if (randomBoolean()) {
            builder.setAnalyzedFields(
                FetchSourceContext.of(
                    true,
                    generateRandomStringArray(10, 10, false, false),
                    generateRandomStringArray(10, 10, false, false)
                )
            );
        }
        if (randomBoolean()) {
            builder.setModelMemoryLimit(ByteSizeValue.of(randomIntBetween(1, 16), randomFrom(ByteSizeUnit.MB, ByteSizeUnit.GB)));
        }
        if (randomBoolean()) {
            builder.setDescription(randomAlphaOfLength(20));
        }
        if (withGeneratedFields) {
            if (randomBoolean()) {
                builder.setCreateTime(Instant.now());
            }
            if (randomBoolean()) {
                builder.setVersion(MlConfigVersion.CURRENT);
            }
        }
        if (randomBoolean()) {
            builder.setAllowLazyStart(randomBoolean());
        }
        if (randomBoolean()) {
            builder.setMaxNumThreads(randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            builder.setMeta(randomMeta());
        }
        return builder;
    }

    public static String randomValidId() {
        CodepointSetGenerator generator = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    // query:match:type stopped being supported in 6.x
    private static final String ANACHRONISTIC_QUERY_DATA_FRAME_ANALYTICS = """
        {
            "id": "old-data-frame",
            "source": {"index":"my-index", "query": {"match" : {"query":"fieldName", "type": "phrase"}}},
            "dest": {"index":"dest-index"},
            "analysis": {"outlier_detection": {"n_neighbors": 10}}
        }""";

    // match_all if parsed, adds default values in the options
    private static final String MODERN_QUERY_DATA_FRAME_ANALYTICS = """
        {
            "id": "data-frame",
            "source": {"index":"my-index", "query": {"match_all" : {}}},
            "dest": {"index":"dest-index"},
            "analysis": {"outlier_detection": {"n_neighbors": 10}}
        }""";

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    public void testQueryConfigStoresUserInputOnly() throws IOException {
        try (XContentParser parser = parser(MODERN_QUERY_DATA_FRAME_ANALYTICS)) {

            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build();
            assertThat(config.getSource().getQuery(), equalTo(Collections.singletonMap(MatchAllQueryBuilder.NAME, Collections.emptyMap())));
        }

        try (XContentParser parser = parser(MODERN_QUERY_DATA_FRAME_ANALYTICS)) {

            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.STRICT_PARSER.apply(parser, null).build();
            assertThat(config.getSource().getQuery(), equalTo(Collections.singletonMap(MatchAllQueryBuilder.NAME, Collections.emptyMap())));
        }
    }

    public void testPastQueryConfigParse() throws IOException {
        try (XContentParser parser = parser(ANACHRONISTIC_QUERY_DATA_FRAME_ANALYTICS)) {

            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build();
            ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> config.getSource().getParsedQuery());
            assertEquals("[match] query doesn't support multiple fields, found [query] and [type]", e.getMessage());
        }

        try (XContentParser parser = parser(ANACHRONISTIC_QUERY_DATA_FRAME_ANALYTICS)) {

            XContentParseException e = expectThrows(
                XContentParseException.class,
                () -> DataFrameAnalyticsConfig.STRICT_PARSER.apply(parser, null).build()
            );
            assertThat(e.getMessage(), containsString("[data_frame_analytics_config] failed to parse field [source]"));
        }
    }

    public void testToXContentForInternalStorage() throws IOException {
        DataFrameAnalyticsConfig.Builder builder = createRandomBuilder("foo");

        // headers are only persisted to cluster state
        Map<String, String> headers = new HashMap<>();
        headers.put("header-name", "header-value");
        builder.setHeaders(headers);
        DataFrameAnalyticsConfig config = builder.build();

        ToXContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));

        BytesReference forClusterstateXContent = XContentHelper.toXContent(config, XContentType.JSON, params, false);
        XContentParser parser = parser(forClusterstateXContent);

        DataFrameAnalyticsConfig parsedConfig = DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build();
        assertThat(parsedConfig.getHeaders(), hasEntry("header-name", "header-value"));

        // headers are not written without the FOR_INTERNAL_STORAGE param
        BytesReference nonClusterstateXContent = XContentHelper.toXContent(config, XContentType.JSON, ToXContent.EMPTY_PARAMS, false);
        parser = parser(nonClusterstateXContent);

        parsedConfig = DataFrameAnalyticsConfig.LENIENT_PARSER.apply(parser, null).build();
        assertThat(parsedConfig.getHeaders().entrySet(), hasSize(0));
    }

    public void testInvalidModelMemoryLimits() {

        DataFrameAnalyticsConfig.Builder builder = new DataFrameAnalyticsConfig.Builder();

        // All these are different ways of specifying a limit that is lower than the minimum
        assertTooSmall(
            expectThrows(
                ElasticsearchStatusException.class,
                () -> builder.setModelMemoryLimit(ByteSizeValue.of(-1, ByteSizeUnit.BYTES)).build()
            )
        );
        assertTooSmall(
            expectThrows(
                ElasticsearchStatusException.class,
                () -> builder.setModelMemoryLimit(ByteSizeValue.of(0, ByteSizeUnit.BYTES)).build()
            )
        );
        assertTooSmall(
            expectThrows(
                ElasticsearchStatusException.class,
                () -> builder.setModelMemoryLimit(ByteSizeValue.of(0, ByteSizeUnit.KB)).build()
            )
        );
        assertTooSmall(
            expectThrows(
                ElasticsearchStatusException.class,
                () -> builder.setModelMemoryLimit(ByteSizeValue.of(0, ByteSizeUnit.MB)).build()
            )
        );
        assertTooSmall(
            expectThrows(
                ElasticsearchStatusException.class,
                () -> builder.setModelMemoryLimit(ByteSizeValue.of(1023, ByteSizeUnit.BYTES)).build()
            )
        );
    }

    public void testNoMemoryCapping() {

        DataFrameAnalyticsConfig uncapped = createRandom("foo");

        ByteSizeValue unlimited = randomBoolean() ? null : ByteSizeValue.ZERO;
        assertThat(
            uncapped.getModelMemoryLimit(),
            equalTo(new DataFrameAnalyticsConfig.Builder(uncapped, unlimited).build().getModelMemoryLimit())
        );
    }

    public void testMemoryCapping() {

        DataFrameAnalyticsConfig defaultLimitConfig = createRandomBuilder("foo").setModelMemoryLimit(null).build();

        ByteSizeValue maxLimit = ByteSizeValue.of(randomIntBetween(500, 1000), ByteSizeUnit.MB);
        if (maxLimit.compareTo(defaultLimitConfig.getModelMemoryLimit()) < 0) {
            assertThat(maxLimit, equalTo(new DataFrameAnalyticsConfig.Builder(defaultLimitConfig, maxLimit).build().getModelMemoryLimit()));
        } else {
            assertThat(
                defaultLimitConfig.getModelMemoryLimit(),
                equalTo(new DataFrameAnalyticsConfig.Builder(defaultLimitConfig, maxLimit).build().getModelMemoryLimit())
            );
        }
    }

    public void testExplicitModelMemoryLimitTooHigh() {

        ByteSizeValue configuredLimit = ByteSizeValue.of(randomIntBetween(5, 10), ByteSizeUnit.GB);
        DataFrameAnalyticsConfig explicitLimitConfig = createRandomBuilder("foo").setModelMemoryLimit(configuredLimit).build();

        ByteSizeValue maxLimit = ByteSizeValue.of(randomIntBetween(500, 1000), ByteSizeUnit.MB);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> new DataFrameAnalyticsConfig.Builder(explicitLimitConfig, maxLimit).build()
        );
        assertThat(e.getMessage(), startsWith("model_memory_limit"));
        assertThat(e.getMessage(), containsString("must be less than the value of the xpack.ml.max_model_memory_limit setting"));
    }

    public void testBuildForExplain() {
        DataFrameAnalyticsConfig.Builder builder = createRandomBuilder("foo");

        DataFrameAnalyticsConfig config = builder.buildForExplain();

        assertThat(config, equalTo(builder.build()));
    }

    public void testBuildForExplain_MissingId() {
        DataFrameAnalyticsConfig.Builder builder = new DataFrameAnalyticsConfig.Builder().setAnalysis(OutlierDetectionTests.createRandom())
            .setSource(DataFrameAnalyticsSourceTests.createRandom())
            .setDest(DataFrameAnalyticsDestTests.createRandom());

        DataFrameAnalyticsConfig config = builder.buildForExplain();

        assertThat(config.getId(), equalTo(DataFrameAnalyticsConfig.BLANK_ID));
    }

    public void testBuildForExplain_MissingDest() {
        DataFrameAnalyticsConfig.Builder builder = new DataFrameAnalyticsConfig.Builder().setId("foo")
            .setAnalysis(OutlierDetectionTests.createRandom())
            .setSource(DataFrameAnalyticsSourceTests.createRandom());

        DataFrameAnalyticsConfig config = builder.buildForExplain();

        assertThat(config.getDest().getIndex(), equalTo(DataFrameAnalyticsConfig.BLANK_DEST_INDEX));
    }

    public void testPreventCreateTimeInjection() throws IOException {
        String json = """
            { "create_time" : 123456789 }, "source" : {"index":"src"}, "dest" : {"index": "dest"},}""";

        try (XContentParser parser = parser(json)) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> DataFrameAnalyticsConfig.STRICT_PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("unknown field [create_time]"));
        }
    }

    public void testPreventVersionInjection() throws IOException {
        String json = """
            { "version" : "7.3.0", "source" : {"index":"src"}, "dest" : {"index": "dest"},}""";

        try (XContentParser parser = parser(json)) {
            Exception e = expectThrows(IllegalArgumentException.class, () -> DataFrameAnalyticsConfig.STRICT_PARSER.apply(parser, null));
            assertThat(e.getMessage(), containsString("unknown field [version]"));
        }
    }

    public void testToXContent_GivenAnalysisWithRandomizeSeedAndVersionIsCurrent() throws IOException {
        Regression regression = new Regression("foo");
        assertThat(regression.getRandomizeSeed(), is(notNullValue()));

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setVersion(MlConfigVersion.CURRENT)
            .setId("test_config")
            .setSource(new DataFrameAnalyticsSource(new String[] { "source_index" }, null, null, null))
            .setDest(new DataFrameAnalyticsDest("dest_index", null))
            .setAnalysis(regression)
            .build();

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            config.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = Strings.toString(builder);
            assertThat(json, containsString("randomize_seed"));
        }
    }

    public void testExtractJobIdFromDocId() {
        assertThat(DataFrameAnalyticsConfig.extractJobIdFromDocId("data_frame_analytics_config-foo"), equalTo("foo"));
        assertThat(
            DataFrameAnalyticsConfig.extractJobIdFromDocId("data_frame_analytics_config-data_frame_analytics_config-foo"),
            equalTo("data_frame_analytics_config-foo")
        );
        assertThat(DataFrameAnalyticsConfig.extractJobIdFromDocId("foo"), is(nullValue()));
    }

    public void testCtor_GivenMaxNumThreadsIsZero() {
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> new DataFrameAnalyticsConfig.Builder().setId("test_config")
                .setSource(new DataFrameAnalyticsSource(new String[] { "source_index" }, null, null, null))
                .setDest(new DataFrameAnalyticsDest("dest_index", null))
                .setAnalysis(new Regression("foo"))
                .setMaxNumThreads(0)
                .build()
        );

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("[max_num_threads] must be a positive integer"));
    }

    public void testCtor_GivenMaxNumThreadsIsNegative() {
        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> new DataFrameAnalyticsConfig.Builder().setId("test_config")
                .setSource(new DataFrameAnalyticsSource(new String[] { "source_index" }, null, null, null))
                .setDest(new DataFrameAnalyticsDest("dest_index", null))
                .setAnalysis(new Regression("foo"))
                .setMaxNumThreads(randomIntBetween(Integer.MIN_VALUE, 0))
                .build()
        );

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("[max_num_threads] must be a positive integer"));
    }

    private static void assertTooSmall(ElasticsearchStatusException e) {
        assertThat(e.getMessage(), startsWith("model_memory_limit must be at least 1kb."));
    }

    private XContentParser parser(String json) throws IOException {
        return JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), json);
    }

    private XContentParser parser(BytesReference json) throws IOException {
        return JsonXContent.jsonXContent.createParser(
            XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()),
            json.streamInput()
        );
    }

    public static Map<String, Object> randomMeta() {
        return rarely() ? null : randomMap(0, 10, () -> {
            String key = randomAlphaOfLengthBetween(1, 10);
            Object value = switch (randomIntBetween(0, 3)) {
                case 0 -> null;
                case 1 -> randomLong();
                case 2 -> randomAlphaOfLengthBetween(1, 10);
                case 3 -> randomMap(0, 3, () -> Tuple.tuple(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
                default -> throw new AssertionError("Error in test code");
            };
            return Tuple.tuple(key, value);
        });
    }
}
