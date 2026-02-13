/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.HttpEntity;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongRangeBlockBuilder;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramBuilder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;
import org.elasticsearch.exponentialhistogram.ReleasableExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ZeroBucket;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.MutableAnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute.FieldName;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohash;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeohex;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StGeotile;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.inference.InferenceService;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParam;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceCommand;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matcher;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsEqual;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.jar.JarInputStream;
import java.util.regex.Pattern;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999;
import static org.elasticsearch.test.ESTestCase.assertEquals;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.fail;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomArray;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomByte;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFloat;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomGaussianDouble;
import static org.elasticsearch.test.ESTestCase.randomIdentifier;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomIp;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomMillisUpToYear9999;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.ESTestCase.randomShort;
import static org.elasticsearch.test.ESTestCase.randomZone;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.CARTESIAN;
import static org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes.GEO;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.IDENTIFIER;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.PATTERN;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.ParamClassification.VALUE;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public final class EsqlTestUtils {

    public static final Literal ONE = new Literal(Source.EMPTY, 1, DataType.INTEGER);
    public static final Literal TWO = new Literal(Source.EMPTY, 2, DataType.INTEGER);
    public static final Literal THREE = new Literal(Source.EMPTY, 3, DataType.INTEGER);
    public static final Literal FOUR = new Literal(Source.EMPTY, 4, DataType.INTEGER);
    public static final Literal FIVE = new Literal(Source.EMPTY, 5, DataType.INTEGER);
    public static final Literal SIX = new Literal(Source.EMPTY, 6, DataType.INTEGER);

    private static final Logger LOGGER = LogManager.getLogger(EsqlTestUtils.class);

    public static Equals equalsOf(Expression left, Expression right) {
        return new Equals(EMPTY, left, right, null);
    }

    public static LessThan lessThanOf(Expression left, Expression right) {
        return new LessThan(EMPTY, left, right, null);
    }

    public static GreaterThan greaterThanOf(Expression left, Expression right) {
        return new GreaterThan(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static NotEquals notEqualsOf(Expression left, Expression right) {
        return new NotEquals(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static LessThanOrEqual lessThanOrEqualOf(Expression left, Expression right) {
        return new LessThanOrEqual(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static GreaterThanOrEqual greaterThanOrEqualOf(Expression left, Expression right) {
        return new GreaterThanOrEqual(EMPTY, left, right, ESTestCase.randomZone());
    }

    public static FieldAttribute findFieldAttribute(LogicalPlan plan, String name) {
        return findFieldAttribute(plan, name, (unused) -> true);
    }

    public static FieldAttribute findFieldAttribute(LogicalPlan plan, String name, Predicate<EsRelation> inThisRelation) {
        Holder<FieldAttribute> result = new Holder<>();
        plan.forEachDown(EsRelation.class, relation -> {
            if (inThisRelation.test(relation) == false) {
                return;
            }
            for (Attribute attr : relation.output()) {
                if (attr.name().equals(name)) {
                    assertNull("Multiple matching field attributes found", result.get());
                    result.set((FieldAttribute) attr);
                    return;
                }
            }
        });
        return result.get();
    }

    public static FieldAttribute getFieldAttribute() {
        return getFieldAttribute("a");
    }

    public static FieldAttribute getFieldAttribute(String name) {
        return getFieldAttribute(name, INTEGER);
    }

    public static FieldAttribute getFieldAttribute(String name, DataType dataType) {
        return new FieldAttribute(EMPTY, name, new EsField(name + "f", dataType, emptyMap(), true, EsField.TimeSeriesFieldType.NONE));
    }

    public static FieldAttribute fieldAttribute() {
        return fieldAttribute(randomAlphaOfLength(10), randomFrom(DataType.types()));
    }

    public static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(EMPTY, name, new EsField(name, type, emptyMap(), randomBoolean(), EsField.TimeSeriesFieldType.NONE));
    }

    public static Literal of(Object value) {
        return of(Source.EMPTY, value);
    }

    /**
     * Utility method for creating 'in-line' Literals (out of values instead of expressions).
     */
    public static Literal of(Source source, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        var dataType = DataType.fromJava(value);
        if (value instanceof String) {
            value = BytesRefs.toBytesRef(value);
        }
        return new Literal(source, value, dataType);
    }

    public static ReferenceAttribute referenceAttribute(String name, DataType type) {
        return new ReferenceAttribute(EMPTY, name, type);
    }

    public static Alias alias(String name, Expression child) {
        return new Alias(EMPTY, name, child);
    }

    public static Mul mul(Expression left, Expression right) {
        return new Mul(EMPTY, left, right);
    }

    public static Range rangeOf(Expression value, Expression lower, boolean includeLower, Expression upper, boolean includeUpper) {
        return new Range(EMPTY, value, lower, includeLower, upper, includeUpper, randomZone());
    }

    public static EsRelation relation() {
        return relation(IndexMode.STANDARD);
    }

    public static EsRelation relation(IndexMode mode) {
        return new EsRelation(EMPTY, randomIdentifier(), mode, Map.of(), Map.of(), Map.of(), List.of());
    }

    /**
     * This version of SearchStats always returns true for all fields for all boolean methods.
     * For custom behaviour either use {@link TestConfigurableSearchStats} or override the specific methods.
     */
    public static class TestSearchStats implements SearchStats {

        @Override
        public boolean exists(FieldName field) {
            return true;
        }

        @Override
        public boolean isIndexed(FieldName field) {
            return exists(field);
        }

        @Override
        public boolean hasDocValues(FieldName field) {
            return exists(field);
        }

        @Override
        public boolean hasExactSubfield(FieldName field) {
            return exists(field);
        }

        @Override
        public boolean supportsLoaderConfig(
            FieldName name,
            BlockLoaderFunctionConfig config,
            MappedFieldType.FieldExtractPreference preference
        ) {
            return true;
        }

        @Override
        public long count() {
            return -1;
        }

        @Override
        public long count(FieldName field) {
            return exists(field) ? -1 : 0;
        }

        @Override
        public long count(FieldName field, BytesRef value) {
            return exists(field) ? -1 : 0;
        }

        @Override
        public Object min(FieldName field) {
            return null;
        }

        @Override
        public Object max(FieldName field) {
            return null;
        }

        @Override
        public boolean isSingleValue(FieldName field) {
            return false;
        }

        @Override
        public boolean canUseEqualityOnSyntheticSourceDelegate(FieldName name, String value) {
            return false;
        }

        @Override
        public Map<ShardId, IndexMetadata> targetShards() {
            return Map.of();
        }
    }

    /**
     * This version of SearchStats can be preconfigured to return true/false for various combinations of the four field settings:
     * <ol>
     *     <li>exists</li>
     *     <li>isIndexed</li>
     *     <li>hasDocValues</li>
     *     <li>hasExactSubfield</li>
     * </ol>
     * The default will return true for all fields. The include/exclude methods can be used to configure the settings for specific fields.
     * If you call 'include' with no fields, it will switch to return false for all fields.
     */
    public static class TestConfigurableSearchStats extends TestSearchStats {
        public enum Config {
            EXISTS,
            INDEXED,
            DOC_VALUES,
            EXACT_SUBFIELD
        }

        private final Map<Config, Set<String>> includes = new HashMap<>();
        private final Map<Config, Set<String>> excludes = new HashMap<>();

        public TestConfigurableSearchStats include(Config key, String... fields) {
            // If this method is called with no fields, it is interpreted to mean include none, so we include a dummy field
            for (String field : fields.length == 0 ? new String[] { "-" } : fields) {
                includes.computeIfAbsent(key, k -> new HashSet<>()).add(field);
                excludes.computeIfAbsent(key, k -> new HashSet<>()).remove(field);
            }
            return this;
        }

        public TestConfigurableSearchStats exclude(Config key, String... fields) {
            for (String field : fields) {
                includes.computeIfAbsent(key, k -> new HashSet<>()).remove(field);
                excludes.computeIfAbsent(key, k -> new HashSet<>()).add(field);
            }
            return this;
        }

        private boolean isConfigationSet(Config config, String field) {
            Set<String> in = includes.getOrDefault(config, Set.of());
            Set<String> ex = excludes.getOrDefault(config, Set.of());
            return (in.isEmpty() || in.contains(field)) && ex.contains(field) == false;
        }

        @Override
        public boolean exists(FieldName field) {
            return isConfigationSet(Config.EXISTS, field.string());
        }

        @Override
        public boolean isIndexed(FieldName field) {
            return isConfigationSet(Config.INDEXED, field.string());
        }

        @Override
        public boolean hasDocValues(FieldName field) {
            return isConfigationSet(Config.DOC_VALUES, field.string());
        }

        @Override
        public boolean hasExactSubfield(FieldName field) {
            return isConfigationSet(Config.EXACT_SUBFIELD, field.string());
        }

        @Override
        public String toString() {
            return "TestConfigurableSearchStats{" + "includes=" + includes + ", excludes=" + excludes + '}';
        }
    }

    public static class TestSearchStatsWithMinMax extends TestSearchStats {

        private final Map<String, Object> minValues;
        private final Map<String, Object> maxValues;

        public TestSearchStatsWithMinMax(Map<String, Object> minValues, Map<String, Object> maxValues) {
            this.minValues = minValues;
            this.maxValues = maxValues;
        }

        @Override
        public Object min(FieldName field) {
            return minValues.get(field.string());
        }

        @Override
        public Object max(FieldName field) {
            return maxValues.get(field.string());
        }
    }

    public static final TestSearchStats TEST_SEARCH_STATS = new TestSearchStats();

    private static final Map<String, Map<String, Column>> TABLES = tables();

    public static final Configuration TEST_CFG = configuration(new QueryPragmas(Settings.EMPTY));

    public static TransportVersion randomMinimumVersion() {
        return TransportVersionUtils.randomCompatibleVersion();
    }

    // TODO: make this even simpler, remove the enrichResolution for tests that do not require it (most tests)
    public static MutableAnalyzerContext testAnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        Map<IndexPattern, IndexResolution> indexResolutions,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution
    ) {
        return testAnalyzerContext(configuration, functionRegistry, indexResolutions, Map.of(), enrichResolution, inferenceResolution);
    }

    /**
     * Analyzer context for a random (but compatible) minimum transport version.
     */
    public static MutableAnalyzerContext testAnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        Map<IndexPattern, IndexResolution> indexResolutions,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution
    ) {
        return testAnalyzerContext(
            configuration,
            functionRegistry,
            indexResolutions,
            lookupResolution,
            enrichResolution,
            inferenceResolution,
            UNMAPPED_FIELDS.defaultValue()
        );
    }

    public static MutableAnalyzerContext testAnalyzerContext(
        Configuration configuration,
        EsqlFunctionRegistry functionRegistry,
        Map<IndexPattern, IndexResolution> indexResolutions,
        Map<String, IndexResolution> lookupResolution,
        EnrichResolution enrichResolution,
        InferenceResolution inferenceResolution,
        UnmappedResolution unmappedResolution
    ) {
        return new MutableAnalyzerContext(
            configuration,
            functionRegistry,
            indexResolutions,
            lookupResolution,
            enrichResolution,
            inferenceResolution,
            randomMinimumVersion(),
            unmappedResolution
        );
    }

    public static LogicalOptimizerContext unboundLogicalOptimizerContext() {
        return new LogicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), randomMinimumVersion());
    }

    public static final Verifier TEST_VERIFIER = new Verifier(new Metrics(new EsqlFunctionRegistry()), new XPackLicenseState(() -> 0L));

    public static final TransportActionServices MOCK_TRANSPORT_ACTION_SERVICES;
    static {
        ClusterService clusterService = createMockClusterService();
        MOCK_TRANSPORT_ACTION_SERVICES = new TransportActionServices(
            createMockTransportService(),
            mock(SearchService.class),
            null,
            clusterService,
            mock(ProjectResolver.class),
            mock(IndexNameExpressionResolver.class),
            null,
            new InferenceService(mock(Client.class), clusterService),
            new BlockFactoryProvider(PlannerUtils.NON_BREAKING_BLOCK_FACTORY),
            new PlannerSettings.Holder(clusterService),
            new CrossProjectModeDecider(Settings.EMPTY)
        );
    }

    private static ClusterService createMockClusterService() {
        var service = mock(ClusterService.class);
        doReturn(new ClusterName("test-cluster")).when(service).getClusterName();
        doReturn(Settings.EMPTY).when(service).getSettings();

        // Create ClusterSettings with the required inference settings
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(InferenceSettings.getSettings());
        settings.addAll(PlannerSettings.settings());
        var clusterSettings = new ClusterSettings(Settings.EMPTY, settings);
        doReturn(clusterSettings).when(service).getClusterSettings();
        return service;
    }

    private static TransportService createMockTransportService() {
        var service = mock(TransportService.class);
        doReturn(createMockThreadPool()).when(service).getThreadPool();
        return service;
    }

    private static ThreadPool createMockThreadPool() {
        var threadPool = mock(ThreadPool.class);
        doReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE).when(threadPool).executor(anyString());
        return threadPool;
    }

    private EsqlTestUtils() {}

    public static Configuration configuration(QueryPragmas pragmas, String query, EsqlStatement statement) {
        return new Configuration(
            statement.setting(QuerySettings.TIME_ZONE),
            Instant.now(),
            Locale.US,
            null,
            null,
            pragmas,
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            query,
            false,
            TABLES,
            System.nanoTime(),
            false,
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            null,
            Map.of()
        );
    }

    public static Configuration configuration(QueryPragmas pragmas, String query) {
        return configuration(pragmas, query, new EsqlStatement(null, List.of()));
    }

    public static Configuration configuration(QueryPragmas pragmas) {
        return configuration(pragmas, StringUtils.EMPTY);
    }

    public static Configuration configuration(String query) {
        return configuration(QueryPragmas.EMPTY, query);
    }

    public static AnalyzerSettings queryClusterSettings() {
        return new AnalyzerSettings(
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
            AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY)
        );
    }

    public static Literal L(Object value) {
        return of(value);
    }

    public static LogicalPlan emptySource() {
        return new LocalRelation(Source.EMPTY, emptyList(), EmptyLocalSupplier.EMPTY);
    }

    public static LogicalPlan localSource(BlockFactory blockFactory, List<Attribute> fields, List<Object> row) {
        return new LocalRelation(
            Source.EMPTY,
            fields,
            LocalSupplier.of(row.isEmpty() ? new Page(0) : new Page(BlockUtils.fromListRow(blockFactory, row)))
        );
    }

    public static <T> T as(Object node, Class<T> type) {
        Assert.assertThat(node, instanceOf(type));
        return type.cast(node);
    }

    public static Limit asLimit(Object node, Integer limitLiteral) {
        return asLimit(node, limitLiteral, null);
    }

    public static Limit asLimit(Object node, Integer limitLiteral, Boolean duplicated) {
        Limit limit = as(node, Limit.class);
        if (limitLiteral != null) {
            assertEquals(as(limit.limit(), Literal.class).value(), limitLiteral);
        }
        if (duplicated != null) {
            assertEquals(limit.duplicated(), duplicated);
        }
        return limit;
    }

    public static Limit asLimit(Object node, Integer limitLiteral, Boolean duplicated, Boolean local) {
        Limit limit = as(node, Limit.class);
        if (limitLiteral != null) {
            assertEquals(as(limit.limit(), Literal.class).value(), limitLiteral);
        }
        if (duplicated != null) {
            assertEquals(limit.duplicated(), duplicated);
        }
        if (local != null) {
            assertEquals(limit.local(), local);
        }
        return limit;
    }

    public static Map<String, EsField> loadMapping(String name) {
        return LoadMapping.loadMapping(name);
    }

    public static String loadUtf8TextFile(String name) {
        try (InputStream textStream = EsqlTestUtils.class.getResourceAsStream(name)) {
            return new String(textStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static EnrichResolution emptyPolicyResolution() {
        return new EnrichResolution();
    }

    public static InferenceResolution emptyInferenceResolution() {
        return InferenceResolution.EMPTY;
    }

    public static SearchStats statsForExistingField(String... names) {
        return fieldMatchingExistOrMissing(true, names);
    }

    public static SearchStats statsForMissingField(String... names) {
        return fieldMatchingExistOrMissing(false, names);
    }

    private static SearchStats fieldMatchingExistOrMissing(boolean exists, String... names) {
        return new TestSearchStats() {
            private final Set<String> fields = Set.of(names);

            @Override
            public boolean exists(FieldName field) {
                return fields.contains(field.string()) == exists;
            }
        };
    }

    public static List<List<Object>> getValuesList(EsqlQueryResponse results) {
        return getValuesList(results.values());
    }

    public static List<List<Object>> getValuesList(Iterator<Iterator<Object>> values) {
        return toList(Iterators.map(values, EsqlTestUtils::toList));
    }

    public static List<List<Object>> getValuesList(Iterable<Iterable<Object>> values) {
        return toList(Iterators.map(values.iterator(), row -> toList(row.iterator())));
    }

    private static <T> List<T> toList(Iterator<T> iterator) {
        var list = new ArrayList<T>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    public static List<String> withDefaultLimitWarning(List<String> warnings) {
        List<String> result = warnings == null ? new ArrayList<>() : new ArrayList<>(warnings);
        result.add("No limit defined, adding default limit of [1000]");
        return result;
    }

    /**
     * Generates a random enrich command with or without explicit parameters
     */
    public static String randomEnrichCommand(String name, Enrich.Mode mode, String matchField, List<String> enrichFields) {
        String onField = " ";
        String withFields = " ";

        List<String> before = new ArrayList<>();
        List<String> after = new ArrayList<>();

        if (randomBoolean()) {
            // => RENAME new_match_field=match_field | ENRICH name ON new_match_field | RENAME new_match_field AS match_field
            String newMatchField = "my_" + matchField;
            before.add("RENAME " + matchField + " AS " + newMatchField);
            onField = " ON " + newMatchField;
            after.add("RENAME " + newMatchField + " AS " + matchField);
        } else if (randomBoolean()) {
            onField = " ON " + matchField;
        }
        if (randomBoolean()) {
            List<String> fields = new ArrayList<>();
            for (String f : enrichFields) {
                if (randomBoolean()) {
                    fields.add(f);
                } else {
                    // ENRICH name WITH new_a=a,b|new_c=c | RENAME new_a AS a | RENAME new_c AS c
                    fields.add("new_" + f + "=" + f);
                    after.add("RENAME new_" + f + " AS " + f);
                }
            }
            withFields = " WITH " + String.join(",", fields);
        }
        String enrich = "ENRICH ";
        if (mode != Enrich.Mode.ANY || randomBoolean()) {
            enrich += " _" + mode + ":";
        }
        enrich += name;
        enrich += onField;
        enrich += withFields;
        List<String> all = new ArrayList<>(before);
        all.add(enrich);
        all.addAll(after);
        return String.join(" | ", all);
    }

    /**
     * "tables" provided in the context for the LOOKUP command. If you
     * add to this, you must also add to {@code EsqlSpecTestCase#tables};
     */
    public static Map<String, Map<String, Column>> tables() {
        BlockFactory factory = new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE);
        Map<String, Map<String, Column>> tables = new TreeMap<>();
        try (
            IntBlock.Builder ints = factory.newIntBlockBuilder(10);
            LongBlock.Builder longs = factory.newLongBlockBuilder(10);
            BytesRefBlock.Builder names = factory.newBytesRefBlockBuilder(10);
        ) {
            for (int i = 0; i < 10; i++) {
                ints.appendInt(i);
                longs.appendLong(i);
                names.appendBytesRef(new BytesRef(numberName(i)));
            }

            IntBlock intsBlock = ints.build();
            LongBlock longsBlock = longs.build();
            BytesRefBlock namesBlock = names.build();
            tables.put(
                "int_number_names",
                table(
                    Map.entry("int", new Column(DataType.INTEGER, intsBlock)),
                    Map.entry("name", new Column(DataType.KEYWORD, namesBlock))
                )
            );
            tables.put(
                "long_number_names",
                table(Map.entry("long", new Column(DataType.LONG, longsBlock)), Map.entry("name", new Column(DataType.KEYWORD, namesBlock)))
            );
        }
        for (boolean hasNull : new boolean[] { true, false }) {
            try (
                DoubleBlock.Builder doubles = factory.newDoubleBlockBuilder(2);
                BytesRefBlock.Builder names = factory.newBytesRefBlockBuilder(2);
            ) {
                doubles.appendDouble(2.03);
                names.appendBytesRef(new BytesRef("two point zero three"));
                doubles.appendDouble(2.08);
                names.appendBytesRef(new BytesRef("two point zero eight"));
                if (hasNull) {
                    doubles.appendDouble(0.0);
                    names.appendNull();
                }
                tables.put(
                    "double_number_names" + (hasNull ? "_with_null" : ""),
                    table(
                        Map.entry("double", new Column(DataType.DOUBLE, doubles.build())),
                        Map.entry("name", new Column(DataType.KEYWORD, names.build()))
                    )
                );
            }
        }
        try (
            BytesRefBlock.Builder aa = factory.newBytesRefBlockBuilder(3);
            BytesRefBlock.Builder ab = factory.newBytesRefBlockBuilder(3);
            IntBlock.Builder na = factory.newIntBlockBuilder(3);
            IntBlock.Builder nb = factory.newIntBlockBuilder(3);
        ) {
            aa.appendBytesRef(new BytesRef("foo"));
            ab.appendBytesRef(new BytesRef("zoo"));
            na.appendInt(1);
            nb.appendInt(-1);

            aa.appendBytesRef(new BytesRef("bar"));
            ab.appendBytesRef(new BytesRef("zop"));
            na.appendInt(10);
            nb.appendInt(-10);

            aa.appendBytesRef(new BytesRef("baz"));
            ab.appendBytesRef(new BytesRef("zoi"));
            na.appendInt(100);
            nb.appendInt(-100);

            aa.appendBytesRef(new BytesRef("foo"));
            ab.appendBytesRef(new BytesRef("foo"));
            na.appendInt(2);
            nb.appendInt(-2);

            tables.put(
                "big",
                table(
                    Map.entry("aa", new Column(DataType.KEYWORD, aa.build())),
                    Map.entry("ab", new Column(DataType.KEYWORD, ab.build())),
                    Map.entry("na", new Column(DataType.INTEGER, na.build())),
                    Map.entry("nb", new Column(DataType.INTEGER, nb.build()))
                )
            );
        }

        return unmodifiableMap(tables);
    }

    /**
     * Builds a table from the provided parameters. This isn't just a call to
     * {@link Map#of} because we want to maintain sort order of the columns
     */
    @SafeVarargs
    public static <T> Map<String, T> table(Map.Entry<String, T>... kv) {
        Map<String, T> table = new LinkedHashMap<>();
        for (Map.Entry<String, T> stringTEntry : kv) {
            table.put(stringTEntry.getKey(), stringTEntry.getValue());
        }
        return table;
    }

    public static String numberName(int i) {
        return switch (i) {
            case 0 -> "zero";
            case 1 -> "one";
            case 2 -> "two";
            case 3 -> "three";
            case 4 -> "four";
            case 5 -> "five";
            case 6 -> "six";
            case 7 -> "seven";
            case 8 -> "eight";
            case 9 -> "nine";
            default -> throw new IllegalArgumentException();
        };
    }

    @SuppressForbidden(reason = "need to open stream")
    public static InputStream inputStream(URL resource) throws IOException {
        URLConnection con = resource.openConnection();
        // do not to cache files (to avoid keeping file handles around)
        con.setUseCaches(false);
        return con.getInputStream();
    }

    public static BufferedReader reader(URL resource) throws IOException {
        return new BufferedReader(new InputStreamReader(inputStream(resource), StandardCharsets.UTF_8));
    }

    /**
     * Returns the classpath resources matching a simple pattern ("*.csv").
     * It supports folders separated by "/" (e.g. "/some/folder/*.txt").
     *
     * Currently able to resolve resources inside the classpath either from:
     * folders in the file-system (typically IDEs) or
     * inside jars (gradle).
     */
    @SuppressForbidden(reason = "classpath discovery")
    public static List<URL> classpathResources(String pattern) throws IOException {
        while (pattern.startsWith("/")) {
            pattern = pattern.substring(1);
        }

        Tuple<String, String> split = pathAndName(pattern);

        // the root folder searched inside the classpath - default is the root classpath
        // default file match
        final String root = split.v1();
        final String filePattern = split.v2();

        String[] resources = System.getProperty("java.class.path").split(System.getProperty("path.separator"));

        List<URL> matches = new ArrayList<>();

        for (String resource : resources) {
            Path path = PathUtils.get(resource);

            // check whether we're dealing with a jar
            // Java 7 java.nio.fileFileSystem can be used on top of ZIPs/JARs but consumes more memory
            // hence the use of the JAR API
            if (path.toString().endsWith(".jar")) {
                try (JarInputStream jar = jarInputStream(path.toUri().toURL())) {
                    ZipEntry entry = null;
                    while ((entry = jar.getNextEntry()) != null) {
                        String name = entry.getName();
                        Tuple<String, String> entrySplit = pathAndName(name);
                        if (root.equals(entrySplit.v1()) && Regex.simpleMatch(filePattern, entrySplit.v2())) {
                            matches.add(new URL("jar:" + path.toUri() + "!/" + name));
                        }
                    }
                }
            }
            // normal file access
            else if (Files.isDirectory(path)) {
                Files.walkFileTree(path, EnumSet.allOf(FileVisitOption.class), 1, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        // remove the path folder from the URL
                        String name = Strings.replace(file.toUri().toString(), path.toUri().toString(), StringUtils.EMPTY);
                        Tuple<String, String> entrySplit = pathAndName(name);
                        if (root.equals(entrySplit.v1()) && Regex.simpleMatch(filePattern, entrySplit.v2())) {
                            matches.add(file.toUri().toURL());
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        }
        return matches;
    }

    @SuppressForbidden(reason = "need to open jar")
    public static JarInputStream jarInputStream(URL resource) throws IOException {
        return new JarInputStream(inputStream(resource));
    }

    public static Tuple<String, String> pathAndName(String string) {
        String folder = StringUtils.EMPTY;
        String file = string;
        int lastIndexOf = string.lastIndexOf('/');
        if (lastIndexOf > 0) {
            folder = string.substring(0, lastIndexOf - 1);
            if (lastIndexOf + 1 < string.length()) {
                file = string.substring(lastIndexOf + 1);
            }
        }
        return new Tuple<>(folder, file);
    }

    /**
     * Generate a random value of the appropriate type to fit into blocks of {@code e}.
     */
    public static Literal randomLiteral(DataType type) {
        return new Literal(Source.EMPTY, switch (type) {
            case BOOLEAN -> randomBoolean();
            case BYTE -> randomByte();
            case SHORT -> randomShort();
            case INTEGER, COUNTER_INTEGER -> randomInt();
            case LONG, COUNTER_LONG -> randomLong();
            case UNSIGNED_LONG -> randomNonNegativeLong();
            case DATE_PERIOD -> Period.of(randomIntBetween(-1000, 1000), randomIntBetween(-13, 13), randomIntBetween(-32, 32));
            case DATETIME -> randomMillisUpToYear9999();
            case DATE_NANOS -> randomLongBetween(0, Long.MAX_VALUE);
            case DOUBLE, SCALED_FLOAT, COUNTER_DOUBLE -> randomDouble();
            case FLOAT -> randomFloat();
            case HALF_FLOAT -> HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(randomFloat()));
            case KEYWORD -> new BytesRef(randomAlphaOfLength(5));
            case IP -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean())));
            case TIME_DURATION -> Duration.ofMillis(randomLongBetween(-604800000L, 604800000L)); // plus/minus 7 days
            case TEXT -> new BytesRef(randomAlphaOfLength(50));
            case VERSION -> randomVersion().toBytesRef();
            case GEO_POINT -> GEO.asWkb(GeometryTestUtils.randomPoint());
            case CARTESIAN_POINT -> CARTESIAN.asWkb(ShapeTestUtils.randomPoint());
            case GEO_SHAPE -> GEO.asWkb(GeometryTestUtils.randomGeometry(randomBoolean()));
            case CARTESIAN_SHAPE -> CARTESIAN.asWkb(ShapeTestUtils.randomGeometry(randomBoolean()));
            case GEOHASH -> StGeohash.unboundedGrid.calculateGridId(
                GeometryTestUtils.randomPoint(),
                randomIntBetween(1, Geohash.PRECISION)
            );
            case GEOTILE -> StGeotile.unboundedGrid.calculateGridId(
                GeometryTestUtils.randomPoint(),
                randomIntBetween(0, GeoTileUtils.MAX_ZOOM)
            );
            case GEOHEX -> StGeohex.unboundedGrid.calculateGridId(GeometryTestUtils.randomPoint(), randomIntBetween(0, H3.MAX_H3_RES));
            case AGGREGATE_METRIC_DOUBLE -> new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(
                randomDouble(),
                randomDouble(),
                randomDouble(),
                randomInt()
            );
            case DATE_RANGE -> {
                var from = randomMillisUpToYear9999();
                var to = randomLongBetween(from + 1, MAX_MILLIS_BEFORE_9999);
                yield new LongRangeBlockBuilder.LongRange(from, to);
            }
            case NULL -> null;
            case SOURCE -> {
                try {
                    yield BytesReference.bytes(
                        JsonXContent.contentBuilder().startObject().field(randomAlphaOfLength(3), randomAlphaOfLength(10)).endObject()
                    ).toBytesRef();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            case TSID_DATA_TYPE -> randomTsId().toBytesRef();
            case HISTOGRAM -> randomHistogram();
            case DENSE_VECTOR -> Arrays.asList(randomArray(10, 10, i -> new Float[10], ESTestCase::randomFloat));
            case EXPONENTIAL_HISTOGRAM -> EsqlTestUtils.randomExponentialHistogram();
            case UNSUPPORTED, OBJECT, DOC_DATA_TYPE -> throw new IllegalArgumentException(
                "can't make random values for [" + type.typeName() + "]"
            );
            case TDIGEST -> EsqlTestUtils.randomTDigest();
        }, type);
    }

    public static ExponentialHistogram randomExponentialHistogram() {
        // TODO(b/133393): allow (index,scale) based zero thresholds as soon as we support them in the block
        // ideally Replace this with the shared random generation in ExponentialHistogramTestUtils
        int numBuckets = randomIntBetween(4, 300);
        boolean hasNegativeValues = randomBoolean();
        boolean hasPositiveValues = randomBoolean();
        boolean hasZeroValues = randomBoolean();
        double[] rawValues = IntStream.concat(
            IntStream.concat(
                hasNegativeValues ? IntStream.range(0, randomIntBetween(1, 1000)).map(i1 -> -1) : IntStream.empty(),
                hasPositiveValues ? IntStream.range(0, randomIntBetween(1, 1000)).map(i1 -> 1) : IntStream.empty()
            ),
            hasZeroValues ? IntStream.range(0, randomIntBetween(1, 100)).map(i1 -> 0) : IntStream.empty()
        ).mapToDouble(sign -> sign * (Math.pow(1_000_000, randomDouble()))).toArray();

        ReleasableExponentialHistogram histo = ExponentialHistogram.create(
            numBuckets,
            ExponentialHistogramCircuitBreaker.noop(),
            rawValues
        );
        // Setup a proper zeroThreshold based on a random chance
        if (histo.zeroBucket().count() > 0 && randomBoolean()) {
            double smallestNonZeroValue = DoubleStream.of(rawValues).map(Math::abs).filter(val -> val != 0).min().orElse(0.0);
            double zeroThreshold = smallestNonZeroValue * randomDouble();
            try (ReleasableExponentialHistogram releaseAfterCopy = histo) {
                ZeroBucket zeroBucket = ZeroBucket.create(zeroThreshold, histo.zeroBucket().count());
                ExponentialHistogramBuilder builder = ExponentialHistogram.builder(histo, ExponentialHistogramCircuitBreaker.noop())
                    .zeroBucket(zeroBucket);
                histo = builder.build();
            }
        }
        // Make the result histogram writeable to allow usage in Literals for testing
        return new WriteableExponentialHistogram(histo);
    }

    public static TDigestHolder randomTDigest() {
        // TODO: This is mostly copied from TDigestFieldMapperTests and BlockTestUtils; refactor it.
        int size = between(1, 100);
        // Note - we use TDigestState to build an actual t-digest for realistic values here
        TDigestState digest = TDigestState.createWithoutCircuitBreaking(100);
        for (int i = 0; i < size; i++) {
            double sample = randomGaussianDouble();
            int count = randomIntBetween(1, Integer.MAX_VALUE);
            digest.add(sample, count);
        }
        List<Double> centroids = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        double sum = 0.0;
        long valueCount = 0L;
        for (Centroid c : digest.centroids()) {
            centroids.add(c.mean());
            counts.add(c.count());
            sum += c.mean() * c.count();
            valueCount += c.count();
        }
        double min = digest.getMin();
        double max = digest.getMax();

        TDigestHolder returnValue = null;
        try {
            returnValue = new TDigestHolder(centroids, counts, min, max, sum, valueCount);
        } catch (IOException e) {
            // This is a test util, so we're just going to fail the test here
            fail(e);
        }
        return returnValue;
    }

    public static BytesRef randomHistogram() {
        List<Double> values = ESTestCase.randomList(randomIntBetween(1, 1000), ESTestCase::randomDouble);
        values.sort(Double::compareTo);
        // Note - we need the three parameter version of random list here to ensure it's always the same length as values
        List<Long> counts = ESTestCase.randomList(values.size(), values.size(), () -> ESTestCase.randomLongBetween(1, Long.MAX_VALUE));
        BytesStreamOutput streamOutput = new BytesStreamOutput();
        try {
            for (int i = 0; i < values.size(); i++) {
                long count = counts.get(i);
                // Presuming I didn't mess up the data generation, we should never generate a zero here, so no need to account for it.
                assert count > 0;
                streamOutput.writeVLong(count);
                streamOutput.writeLong(Double.doubleToRawLongBits(values.get(i)));
            }
            BytesRef docValue = streamOutput.bytes().toBytesRef();
            return docValue;
        } catch (IOException e) {
            // This is a test util, so we're just going to fail the test here
            fail(e);
        }
        throw new IllegalArgumentException("Unreachable");
    }

    static Version randomVersion() {
        // TODO degenerate versions and stuff
        return switch (between(0, 2)) {
            case 0 -> new Version(Integer.toString(between(0, 100)));
            case 1 -> new Version(between(0, 100) + "." + between(0, 100));
            case 2 -> new Version(between(0, 100) + "." + between(0, 100) + "." + between(0, 100));
            default -> throw new IllegalArgumentException();
        };
    }

    static BytesReference randomTsId() {
        RoutingPathFields routingPathFields = new RoutingPathFields(null);

        int numDimensions = randomIntBetween(1, 4);
        for (int i = 0; i < numDimensions; i++) {
            String fieldName = "dim" + i;
            if (randomBoolean()) {
                routingPathFields.addString(fieldName, randomAlphaOfLength(randomIntBetween(3, 10)));
            } else {
                routingPathFields.addLong(fieldName, randomLongBetween(1, 1000));
            }
        }

        return routingPathFields.buildHash();
    }

    // lifted from org.elasticsearch.http.HttpClientStatsTrackerTests
    public static String randomizeCase(String s) {
        final char[] chars = s.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            chars[i] = randomizeCase(chars[i]);
        }
        return new String(chars);
    }

    private static char randomizeCase(char c) {
        return switch (between(1, 3)) {
            case 1 -> Character.toUpperCase(c);
            case 2 -> Character.toLowerCase(c);
            default -> c;
        };
    }

    public static WildcardLike wildcardLike(Expression left, String exp) {
        return new WildcardLike(EMPTY, left, new WildcardPattern(exp), false);
    }

    public static RLike rlike(Expression left, String exp) {
        return new RLike(EMPTY, left, new RLikePattern(exp));
    }

    public static QueryParams paramsAsConstant(String key, Object value) {
        return new QueryParams(List.of(paramAsConstant(key, value)));
    }

    public static QueryParams paramsAsConstant(Map<String, Object> params) {
        return new QueryParams(params.entrySet().stream().map(e -> paramAsConstant(e.getKey(), e.getValue())).toList());
    }

    public static QueryParam paramAsConstant(String name, Object value) {
        return new QueryParam(name, value, DataType.fromJava(value), VALUE);
    }

    public static QueryParam paramAsIdentifier(String name, Object value) {
        return new QueryParam(name, value, NULL, IDENTIFIER);
    }

    public static QueryParam paramAsPattern(String name, Object value) {
        return new QueryParam(name, value, NULL, PATTERN);
    }

    /**
     * Asserts that:
     * 1. Cancellation exceptions are ignored when more relevant exceptions exist.
     * 2. Transport exceptions are unwrapped, and the actual causes are reported to users.
     */
    public static void assertEsqlFailure(Exception e) {
        assertNotNull(e);
        var cancellationFailure = ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof TaskCancelledException).orElse(null);
        assertNull("cancellation exceptions must be ignored", cancellationFailure);
        ExceptionsHelper.unwrapCausesAndSuppressed(e, t -> t instanceof RemoteTransportException)
            .ifPresent(transportFailure -> assertNull("remote transport exception must be unwrapped", transportFailure.getCause()));
    }

    public static <T> T singleValue(Collection<T> collection) {
        return singleValue("", collection);
    }

    public static <T> T singleValue(String reason, Collection<T> collection) {
        assertThat(reason, collection, hasSize(1));
        return collection.iterator().next();
    }

    public static Attribute getAttributeByName(Collection<Attribute> attributes, String name) {
        return attributes.stream().filter(attr -> attr.name().equals(name)).findAny().orElse(null);
    }

    public static Map<String, Object> jsonEntityToMap(HttpEntity entity) throws IOException {
        return entityToMap(entity, XContentType.JSON);
    }

    public static Map<String, Object> entityToMap(HttpEntity entity, XContentType expectedContentType) throws IOException {
        try (InputStream content = entity.getContent()) {
            XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
            assertEquals(expectedContentType, xContentType);
            return XContentHelper.convertToMap(xContentType.xContent(), content, false /* ordered */);
        }
    }

    /**
     * Errors from remotes are wrapped in RemoteException while the ones from the local cluster
     * aren't. This utility method is useful for unwrapping in such cases.
     * @param e Exception to unwrap.
     * @return Cause of RemoteException, else the error itself.
     */
    public static Exception unwrapIfWrappedInRemoteException(Exception e) {
        if (e instanceof RemoteException rce) {
            return (Exception) rce.getCause();
        } else {
            return e;
        }
    }

    public static void assertEqualsIgnoringIds(Object expected, Object actual) {
        assertEqualsIgnoringIds("", expected, actual);
    }

    public static void assertEqualsIgnoringIds(String reason, Object expected, Object actual) {
        assertThat(reason, actual, equalToIgnoringIds(expected));
    }

    /**
     * Returns a matcher that matches if the examined object is logically equal to the specified
     * operand, ignoring the {@link NameId}s of any {@link NamedExpression}s (e.g. {@link Alias} and {@link Attribute}).
     */
    public static <T> org.hamcrest.Matcher<T> equalToIgnoringIds(T operand) {
        return new IsEqualIgnoringIds<T>(operand);
    }

    @SafeVarargs
    public static <T> org.hamcrest.Matcher<java.lang.Iterable<? extends T>> containsIgnoringIds(T... items) {
        List<Matcher<? super T>> matchers = new ArrayList<>();
        for (T item : items) {
            matchers.add(equalToIgnoringIds(item));
        }

        return new IsIterableContainingInOrder<>(matchers);
    }

    @SafeVarargs
    public static <T> org.hamcrest.Matcher<java.lang.Iterable<? extends T>> containsInAnyOrderIgnoringIds(T... items) {
        List<Matcher<? super T>> matchers = new ArrayList<>();
        for (T item : items) {
            matchers.add(equalToIgnoringIds(item));
        }

        return new IsIterableContainingInAnyOrder<>(matchers);
    }

    private static class IsEqualIgnoringIds<T> extends IsEqual<T> {
        @SuppressWarnings("unchecked")
        IsEqualIgnoringIds(T operand) {
            super((T) ignoreIds(operand));
        }

        @Override
        public boolean matches(Object actualValue) {
            return super.matches(ignoreIds(actualValue));
        }
    }

    public static Object ignoreIds(Object node) {
        return switch (node) {
            case Expression expression -> ignoreIdsInExpression(expression);
            case LogicalPlan plan -> ignoreIdsInLogicalPlan(plan);
            case PhysicalPlan pplan -> ignoreIdsInPhysicalPlan(pplan);
            case List<?> list -> list.stream().map(EsqlTestUtils::ignoreIds).toList();
            case null, default -> node;
        };
    }

    private static final NameId DUMMY_ID = new NameId();

    private static Expression ignoreIdsInExpression(Expression expression) {
        return expression.transformDown(
            NamedExpression.class,
            ne -> ne instanceof Alias alias ? alias.withId(DUMMY_ID) : ne instanceof Attribute attr ? attr.withId(DUMMY_ID) : ne
        );
    }

    private static LogicalPlan ignoreIdsInLogicalPlan(LogicalPlan plan) {
        if (plan instanceof Explain explain) {
            return new Explain(explain.source(), ignoreIdsInLogicalPlan(explain.query()));
        }

        return plan.transformExpressionsDown(
            NamedExpression.class,
            ne -> ne instanceof Alias alias ? alias.withId(DUMMY_ID) : ne instanceof Attribute attr ? attr.withId(DUMMY_ID) : ne
        );
    }

    private static PhysicalPlan ignoreIdsInPhysicalPlan(PhysicalPlan plan) {
        PhysicalPlan ignoredInPhysicalNodes = plan.transformExpressionsDown(
            NamedExpression.class,
            ne -> ne instanceof Alias alias ? alias.withId(DUMMY_ID) : ne instanceof Attribute attr ? attr.withId(DUMMY_ID) : ne
        );
        return ignoredInPhysicalNodes.transformDown(FragmentExec.class, fragmentExec -> {
            LogicalPlan fragment = fragmentExec.fragment();
            LogicalPlan ignoredInFragment = ignoreIdsInLogicalPlan(fragment);
            return fragmentExec.withFragment(ignoredInFragment);
        });
    }

    private static final Pattern SET_SPLIT_PATTERN = Pattern.compile("^(\\s*SET\\b[^;]+;)+\\s*\\b", Pattern.CASE_INSENSITIVE);

    /**
     * Checks if a query contains any of the specified indices in its source command.
     * This is useful for determining if a query uses indices that are loaded into both clusters
     * (like enrich source indices or lookup indices), which may require special handling.
     *
     * @param query The ESQL query to check
     * @param indicesToCheck Set of index names to check for (case-insensitive)
     * @return true if the query contains any of the specified indices, false otherwise
     */
    public static boolean queryContainsIndices(String query, Set<String> indicesToCheck) {
        String[] commands = query.split("\\|");
        // remove subqueries
        String first = commands[0].split(",\\s+\\(")[0].trim();
        // Split "SET a=b; FROM x" into "SET a=b; " and "FROM x"
        var setMatcher = SET_SPLIT_PATTERN.matcher(first);
        int lastSetDelimiterPosition = -1;
        if (setMatcher.find()) {
            lastSetDelimiterPosition = setMatcher.end();
        }
        String afterSetStatements = lastSetDelimiterPosition == -1 ? first : first.substring(lastSetDelimiterPosition);
        // Split "FROM a, b, c" into "FROM" and "a, b, c"
        String[] commandParts = afterSetStatements.trim().split("\\s+", 2);
        String command = commandParts[0].trim();
        if (SourceCommand.isSourceCommand(command) && commandParts.length > 1) {
            String[] indices = EsqlParser.INSTANCE.parseQuery(afterSetStatements)
                .collect(UnresolvedRelation.class)
                .getFirst()
                .indexPattern()
                .indexPattern()
                .split(",");
            for (String index : indices) {
                String indexName = index.trim().toLowerCase(Locale.ROOT);
                if (indicesToCheck.contains(indexName)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String addRemoteIndices(String query, Set<String> lookupIndices, boolean onlyRemotes) {
        String[] commands = query.split("\\|");
        // remove subqueries
        String first = commands[0].split(",\\s+\\(")[0].trim();

        // Split "SET a=b; FROM x" into "SET a=b; " and "FROM x"
        var setMatcher = SET_SPLIT_PATTERN.matcher(first);
        int lastSetDelimiterPosition = -1;
        if (setMatcher.find()) {
            lastSetDelimiterPosition = setMatcher.end();
        }
        String setStatements = lastSetDelimiterPosition == -1 ? "" : first.substring(0, lastSetDelimiterPosition);
        String afterSetStatements = lastSetDelimiterPosition == -1 ? first : first.substring(lastSetDelimiterPosition);

        // Split "FROM a, b, c" into "FROM" and "a, b, c"
        String[] commandParts = afterSetStatements.trim().split("\\s+", 2);

        String command = commandParts[0].trim();
        assert command.equalsIgnoreCase("set") == false : "didn't correctly extract the SET statement from the query";
        if (SourceCommand.isSourceCommand(command)) {
            String commandArgs = commandParts[1].trim();
            String[] indices = EsqlParser.INSTANCE.parseQuery(afterSetStatements)
                .collect(UnresolvedRelation.class)
                .getFirst()
                .indexPattern()
                .indexPattern()
                .split(",");
            // This method may be called multiple times on the same testcase when using @Repeat
            boolean alreadyConverted = Arrays.stream(indices).anyMatch(i -> i.trim().startsWith("*:"));
            if (alreadyConverted == false) {
                if (queryContainsIndices(query, lookupIndices)) {
                    // If the query contains lookup indices, use only remotes to avoid duplication
                    onlyRemotes = true;
                }
                int i = 0;
                for (String index : indices) {
                    i = commandArgs.indexOf(index, i);
                    String newIndex = unquoteAndRequoteAsRemote(index.trim(), onlyRemotes);
                    if (i >= 0) {
                        commandArgs = commandArgs.substring(0, i) + newIndex + commandArgs.substring(i + index.length());
                    } else if (command.equalsIgnoreCase("PROMQL")) {
                        // PROMQL queries may not list indices explicitly, relying on the default value
                        newIndex = "index=" + newIndex + " ";
                        commandArgs = newIndex + commandArgs;
                    } else {
                        throw new IllegalStateException("Could not find index [" + index + "] in command arguments [" + commandArgs + "]");
                    }
                    i += newIndex.length();
                }
                String newFirstCommand = command + " " + commandArgs;
                String finalQuery = (setStatements + newFirstCommand.trim() + query.substring(first.length()));
                assert query.split("\n").length == finalQuery.split("\n").length
                    : "the final query should have the same lines for warnings to work";
                return finalQuery;
            }
        }
        return query;
    }

    /**
     * Since partial quoting is prohibited, we need to take the index name, unquote it,
     * convert it to a remote index, and then requote it. For example, "employees" is unquoted,
     * turned into the remote index *:employees, and then requoted to get "*:employees".
     * @param index Name of the index.
     * @param asRemoteIndexOnly If the return needs to be in the form of "*:idx,idx" or "*:idx".
     * @return A remote index pattern that's requoted.
     */
    private static String unquoteAndRequoteAsRemote(String index, boolean asRemoteIndexOnly) {
        index = index.trim();

        int numOfQuotes = 0;
        for (; numOfQuotes < index.length(); numOfQuotes++) {
            if (index.charAt(numOfQuotes) != '"') {
                break;
            }
        }

        String unquoted = unquote(index, numOfQuotes);
        if (asRemoteIndexOnly) {
            return quote("*:" + unquoted, numOfQuotes);
        } else {
            return quote("*:" + unquoted + "," + unquoted, numOfQuotes);
        }
    }

    private static String quote(String index, int numOfQuotes) {
        return "\"".repeat(numOfQuotes) + index + "\"".repeat(numOfQuotes);
    }

    private static String unquote(String index, int numOfQuotes) {
        return index.substring(numOfQuotes, index.length() - numOfQuotes);
    }

    /**
     * Convert index patterns and subqueries in FROM commands to use remote indices.
     */
    public static String convertSubqueryToRemoteIndices(String testQuery) {
        String query = testQuery;
        // find the main from command, ignoring pipes inside subqueries
        List<String> mainFromCommandAndTheRest = splitIgnoringParentheses(query, "|");
        String mainFrom = mainFromCommandAndTheRest.get(0).strip();
        List<String> theRest = mainFromCommandAndTheRest.size() > 1
            ? mainFromCommandAndTheRest.subList(1, mainFromCommandAndTheRest.size())
            : List.of();
        // check for metadata in the main from command
        List<String> mainFromCommandWithMetadata = splitIgnoringParentheses(mainFrom, "metadata");
        mainFrom = mainFromCommandWithMetadata.get(0).strip();
        // if there is metadata, we need to add it back later
        String metadata = mainFromCommandWithMetadata.size() > 1 ? " metadata " + mainFromCommandWithMetadata.get(1) : "";
        // the main from command could be a comma separated list of index patterns, and subqueries
        List<String> indexPatternsAndSubqueries = splitIgnoringParentheses(mainFrom, ",");
        List<String> transformed = new ArrayList<>();
        for (String indexPatternOrSubquery : indexPatternsAndSubqueries) {
            // remove the from keyword if it's there
            indexPatternOrSubquery = indexPatternOrSubquery.strip();
            if (indexPatternOrSubquery.toLowerCase(Locale.ROOT).startsWith("from ")) {
                indexPatternOrSubquery = indexPatternOrSubquery.strip().substring(5);
            }
            // substitute the index patterns or subquery with remote index patterns
            if (isSubquery(indexPatternOrSubquery)) {
                // it's a subquery, we need to process it recursively
                String subquery = indexPatternOrSubquery.strip().substring(1, indexPatternOrSubquery.length() - 1);
                String transformedSubquery = convertSubqueryToRemoteIndices(subquery);
                transformed.add("(" + transformedSubquery + ")");
            } else {
                // It's an index pattern, we need to convert it to remote index pattern.
                String remoteIndex = unquoteAndRequoteAsRemote(indexPatternOrSubquery, false);
                transformed.add(remoteIndex);
            }
        }
        // rebuild from command from transformed index patterns and subqueries
        String transformedFrom = "FROM " + String.join(", ", transformed) + metadata;
        // rebuild the whole query
        mainFromCommandAndTheRest.set(0, transformedFrom);
        testQuery = String.join(" | ", mainFromCommandAndTheRest);

        LOGGER.trace("Transform query: \nFROM: {}\nTO:   {}", query, testQuery);
        return testQuery;
    }

    /**
     * Checks if the given string is a subquery (enclosed in parentheses).
     */
    private static boolean isSubquery(String indexPatternOrSubquery) {
        String trimmed = indexPatternOrSubquery.strip();
        return trimmed.startsWith("(") && trimmed.endsWith(")");
    }

    /**
     * Splits the input string by the given delimiter, ignoring delimiters inside parentheses.
     */
    public static List<String> splitIgnoringParentheses(String input, String delimiter) {
        List<String> results = new ArrayList<>();
        if (input == null || input.isEmpty()) return results;

        int depth = 0; // parentheses nesting
        int lastSplit = 0;
        int delimiterLength = delimiter.length();

        for (int i = 0; i <= input.length() - delimiterLength; i++) {
            char c = input.charAt(i);

            if (c == '(') {
                depth++;
            } else if (c == ')') {
                if (depth > 0) depth--;
            }

            // check delimiter only outside parentheses
            if (depth == 0) {
                boolean match;
                if (delimiter.length() == 1) {
                    match = c == delimiter.charAt(0);
                } else {
                    match = input.regionMatches(true, i, delimiter, 0, delimiterLength);
                }

                if (match) {
                    results.add(input.substring(lastSplit, i).trim());
                    lastSplit = i + delimiterLength;
                    i += delimiterLength - 1; // skip the delimiter
                }
            }
        }
        // add remaining part
        results.add(input.substring(lastSplit).trim());

        return results;
    }
}
