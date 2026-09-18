/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.SignificantTermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class AggregatorFactoriesTests extends ESTestCase {
    private NamedXContentRegistry xContentRegistry;

    @Before
    public void initXContentRegistry() throws Exception {
        Settings settings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();
        xContentRegistry = new NamedXContentRegistry(new SearchModule(settings, emptyList()).getNamedXContents());
    }

    public void testGetAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo"));
        Collection<AggregationBuilder> aggregatorFactories = builder.getAggregatorFactories();
        assertThat(aggregatorFactories.size(), equalTo(1));
        expectThrows(UnsupportedOperationException.class, () -> aggregatorFactories.add(AggregationBuilders.avg("bar")));
    }

    public void testGetPipelineAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addPipelineAggregator(
            PipelineAggregatorBuilders.avgBucket("foo", "path1")
        );
        Collection<PipelineAggregationBuilder> pipelineAggregatorFactories = builder.getPipelineAggregatorFactories();
        assertThat(pipelineAggregatorFactories.size(), equalTo(1));
        expectThrows(
            UnsupportedOperationException.class,
            () -> pipelineAggregatorFactories.add(PipelineAggregatorBuilders.avgBucket("bar", "path2"))
        );
    }

    public void testTwoTypes() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("in_stock")
            .startObject("filter")
            .startObject("range")
            .startObject("stock")
            .field("gt", 0)
            .endObject()
            .endObject()
            .endObject()
            .startObject("terms")
            .field("field", "stock")
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Found two aggregation type definitions in [in_stock]: [filter] and [terms]"));
    }

    public void testInvalidAggregationName() throws Exception {
        Matcher matcher = Pattern.compile("[^\\[\\]>]+").matcher("");
        String name;
        Random rand = random();
        int len = randomIntBetween(1, 5);
        char[] word = new char[len];
        while (true) {
            for (int i = 0; i < word.length; i++) {
                word[i] = (char) rand.nextInt(127);
            }
            name = String.valueOf(word);
            if (matcher.reset(name).matches() == false) {
                break;
            }
        }

        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject(name)
            .startObject("filter")
            .startObject("range")
            .startObject("stock")
            .field("gt", 0)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Invalid aggregation name [" + name + "]"));
    }

    public void testMissingName() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("by_date")
            .startObject("date_histogram")
            .field("field", "timestamp")
            .field("calendar_interval", "month")
            .endObject()
            .startObject("aggs")
            // the aggregation name is missing
            // .startObject("tag_count")
            .startObject("cardinality")
            .field("field", "tag")
            .endObject()
            // .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Expected [START_OBJECT] under [field], but got a [VALUE_STRING] in [cardinality]"));
    }

    public void testMissingType() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("by_date")
            .startObject("date_histogram")
            .field("field", "timestamp")
            .field("calendar_interval", "month")
            .endObject()
            .startObject("aggs")
            .startObject("tag_count")
            // the aggregation type is missing
            // .startObject("cardinality")
            .field("field", "tag")
            // .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Expected [START_OBJECT] under [field], but got a [VALUE_STRING] in [tag_count]"));
    }

    public void testInvalidType() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("by_date")
            .startObject("date_histogram")
            .field("field", "timestamp")
            .field("calendar_interval", "month")
            .endObject()
            .startObject("aggs")
            .startObject("tags")
            // the aggregation type is invalid
            .startObject("term")
            .field("field", "tag")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Unknown aggregation type [term] did you mean [terms]?"));
    }

    public void testMaxNestedDepth() throws Exception {
        int maxDepth = defaultMaxNestedDepth();
        assertNestedDepthAccepted(maxDepth);
        ParsingException e = expectMaxNestedDepthExceeded(maxDepth + 1);
        assertThat(
            e.getMessage(),
            equalTo(
                "The nested depth of the aggregations exceeds the maximum nested depth for aggregations of ["
                    + maxDepth
                    + "] set in ["
                    + AggregatorFactories.MAX_NESTED_DEPTH_SETTING.getKey()
                    + "]"
            )
        );
    }

    public void testMaxNestedDepthCustomLimit() throws Exception {
        int maxDepth = randomIntBetween(1, 5);
        AggregatorFactories.setMaxNestedDepth(maxDepth);
        try {
            assertNestedDepthAccepted(maxDepth);
            ParsingException e = expectMaxNestedDepthExceeded(maxDepth + 1);
            assertThat(e.getMessage(), containsString("exceeds the maximum nested depth for aggregations of [" + maxDepth + "]"));
        } finally {
            AggregatorFactories.setMaxNestedDepth(defaultMaxNestedDepth());
        }
    }

    public void testMaxNestedDepthFromNodeSettings() throws Exception {
        int maxDepth = randomIntBetween(1, 5);
        Settings settings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put(AggregatorFactories.MAX_NESTED_DEPTH_SETTING.getKey(), maxDepth)
            .build();
        try {
            new SearchModule(settings, emptyList());
            assertNestedDepthAccepted(maxDepth);
            ParsingException e = expectMaxNestedDepthExceeded(maxDepth + 1);
            assertThat(e.getMessage(), containsString("exceeds the maximum nested depth for aggregations of [" + maxDepth + "]"));
        } finally {
            AggregatorFactories.setMaxNestedDepth(defaultMaxNestedDepth());
        }
    }

    public void testMaxNestedDepthSettingIsDynamic() {
        assertTrue(
            "search.aggs.max_nested_depth must stay dynamic so operators can adjust it without a node restart",
            AggregatorFactories.MAX_NESTED_DEPTH_SETTING.isDynamic()
        );
    }

    public void testMaxNestedDepthSettingRejectsValuesAboveHardLimit() {
        int tooHigh = AggregatorFactories.MAX_NESTED_DEPTH_HARD_LIMIT + 1;
        Settings settings = Settings.builder().put(AggregatorFactories.MAX_NESTED_DEPTH_SETTING.getKey(), tooHigh).build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AggregatorFactories.MAX_NESTED_DEPTH_SETTING.get(settings)
        );
        assertThat(e.getMessage(), containsString(String.valueOf(AggregatorFactories.MAX_NESTED_DEPTH_HARD_LIMIT)));
    }

    public void testMaxNestedDepthSettingAcceptsHardLimit() {
        Settings settings = Settings.builder()
            .put(AggregatorFactories.MAX_NESTED_DEPTH_SETTING.getKey(), AggregatorFactories.MAX_NESTED_DEPTH_HARD_LIMIT)
            .build();
        assertThat(AggregatorFactories.MAX_NESTED_DEPTH_SETTING.get(settings), equalTo(AggregatorFactories.MAX_NESTED_DEPTH_HARD_LIMIT));
    }

    public void testMaxNestedDepthEnforcedAtBuildTime() {
        int maxDepth = defaultMaxNestedDepth();
        AggregatorFactories.Builder tooDeep = nestedTermsBuilder(maxDepth + 1);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> tooDeep.build(null, null));
        assertThat(e.getMessage(), containsString("exceeds the maximum nested depth for aggregations of [" + maxDepth + "]"));
    }

    public void testMaxNestedDepthEnforcedAfterTransportSerialization() throws IOException {
        int maxDepth = defaultMaxNestedDepth();
        AggregatorFactories.Builder tooDeep = nestedTermsBuilder(maxDepth + 1);
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            new SearchModule(Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build(), emptyList())
                .getNamedWriteables()
        );
        AggregatorFactories.Builder roundTripped;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            tooDeep.writeTo(out);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                roundTripped = new AggregatorFactories.Builder(in);
            }
        }
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> roundTripped.build(null, null));
        assertThat(e.getMessage(), containsString("exceeds the maximum nested depth for aggregations of [" + maxDepth + "]"));
    }

    public void testMaxNestedDepthEnforcedAtValidationTime() {
        int maxDepth = defaultMaxNestedDepth();
        ActionRequestValidationException e = nestedTermsBuilder(maxDepth + 1).validate(null);
        assertNotNull(e);
        assertThat(e.getMessage(), containsString("exceeds the maximum nested depth for aggregations of [" + maxDepth + "]"));
    }

    public void testValidationOfVeryDeepTreeDoesNotOverflowTheStack() {
        int maxDepth = defaultMaxNestedDepth();
        ActionRequestValidationException e = veryDeeplyNestedTermsBuilder().validate(null);
        assertNotNull(e);
        assertThat(e.getMessage(), containsString(maxNestedDepthExceededMessage(maxDepth)));
    }

    public void testValidationOfVeryDeepTreeInSearchRequestDoesNotOverflowTheStack() {
        int maxDepth = defaultMaxNestedDepth();
        SearchRequest request = new SearchRequest().source(new SearchSourceBuilder().aggregationsBuilder(veryDeeplyNestedTermsBuilder()));
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), containsString(maxNestedDepthExceededMessage(maxDepth)));
    }

    public void testMaxNestedDepthEnforcedAtValidationTimeRegardlessOfAllowPartialSearchResults() {
        int maxDepth = defaultMaxNestedDepth();
        SearchRequest request = new SearchRequest().source(new SearchSourceBuilder().aggregationsBuilder(nestedTermsBuilder(maxDepth + 1)))
            .allowPartialSearchResults(true);
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), containsString("exceeds the maximum nested depth for aggregations of [" + maxDepth + "]"));
    }

    public void testMaxNestedDepthBoundaryAcceptedAtValidationTime() {
        assertNull(nestedTermsBuilder(defaultMaxNestedDepth()).validate(null));
    }

    public void testMaxNestedDepthBoundaryAcceptedAtBuildTime() {
        AggregatorFactories.Builder atLimit = nestedTermsBuilder(defaultMaxNestedDepth());
        try {
            atLimit.build(null, null);
        } catch (Exception e) {
            assertThat(
                "a tree at the limit must not be rejected by the depth guard",
                e.getMessage() == null ? "" : e.getMessage(),
                not(containsString("maximum nested depth"))
            );
        }
    }

    private static AggregatorFactories.Builder veryDeeplyNestedTermsBuilder() {
        TermsAggregationBuilder leaf = new TermsAggregationBuilder("a0").field("f");
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(leaf);
        for (int i = 1; i < 20_000; i++) {
            TermsAggregationBuilder child = new TermsAggregationBuilder("a" + i).field("f");
            leaf.subAggregation(child);
            leaf = child;
        }
        return builder;
    }

    private static AggregatorFactories.Builder nestedTermsBuilder(int depth) {
        TermsAggregationBuilder root = new TermsAggregationBuilder("a0").field("f");
        TermsAggregationBuilder parent = root;
        for (int i = 1; i < depth; i++) {
            TermsAggregationBuilder child = new TermsAggregationBuilder("a" + i).field("f");
            parent.subAggregation(child);
            parent = child;
        }
        return new AggregatorFactories.Builder().addAggregator(root);
    }

    private static int defaultMaxNestedDepth() {
        return AggregatorFactories.MAX_NESTED_DEPTH_SETTING.getDefault(Settings.EMPTY);
    }

    /**
     * Mirrors {@code AggregatorFactories.maxNestedDepthExceededMessage()} so tests can assert against the full
     * expected message, including the setting name, rather than a hand-typed partial substring.
     */
    private static String maxNestedDepthExceededMessage(int maxDepth) {
        return "The nested depth of the aggregations exceeds the maximum nested depth for aggregations of ["
            + maxDepth
            + "] set in ["
            + AggregatorFactories.MAX_NESTED_DEPTH_SETTING.getKey()
            + "]";
    }

    private void assertNestedDepthAccepted(int depth) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, nestedTermsAggs(depth, false))) {
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertThat(AggregatorFactories.parseAggregators(parser).count(), equalTo(1));
        }
    }

    private ParsingException expectMaxNestedDepthExceeded(int depth) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, nestedTermsAggs(depth, false))) {
            assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
            return expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        }
    }

    private static String nestedTermsAggs(int depth, boolean trailingEmptyAggs) {
        StringBuilder aggs = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            if (i > 0) {
                aggs.append(",\"aggs\":");
            }
            aggs.append("{\"a").append(i).append("\":{\"terms\":{\"field\":\"f\"}");
        }
        if (trailingEmptyAggs) {
            aggs.append(",\"aggs\":{}");
        }
        return aggs.append("}}".repeat(depth)).toString();
    }

    public void testRewriteAggregation() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytesReference;
        try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType)) {
            builder.startObject();
            {
                builder.startObject("terms");
                {
                    builder.array("title", "foo");
                }
                builder.endObject();
            }
            builder.endObject();
            bytesReference = BytesReference.bytes(builder);
        }
        FilterAggregationBuilder filterAggBuilder = new FilterAggregationBuilder("titles", new WrapperQueryBuilder(bytesReference));
        BucketScriptPipelineAggregationBuilder pipelineAgg = new BucketScriptPipelineAggregationBuilder("const", new Script("1"));
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(filterAggBuilder)
            .addPipelineAggregator(pipelineAgg);
        AggregatorFactories.Builder rewritten = builder.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertNotSame(builder, rewritten);
        Collection<AggregationBuilder> aggregatorFactories = rewritten.getAggregatorFactories();
        assertEquals(1, aggregatorFactories.size());
        assertThat(aggregatorFactories.iterator().next(), instanceOf(FilterAggregationBuilder.class));
        FilterAggregationBuilder rewrittenFilterAggBuilder = (FilterAggregationBuilder) aggregatorFactories.iterator().next();
        assertNotSame(filterAggBuilder, rewrittenFilterAggBuilder);
        assertNotEquals(filterAggBuilder, rewrittenFilterAggBuilder);
        // Check the filter was rewritten from a wrapper query to a terms query
        QueryBuilder rewrittenFilter = rewrittenFilterAggBuilder.getFilter();
        assertThat(rewrittenFilter, instanceOf(TermsQueryBuilder.class));

        // Check that a further rewrite returns the same aggregation factories builder
        AggregatorFactories.Builder secondRewritten = rewritten.rewrite(new QueryRewriteContext(parserConfig(), null, () -> 0L));
        assertSame(rewritten, secondRewritten);
    }

    public void testRewritePipelineAggregationUnderAggregation() throws Exception {
        FilterAggregationBuilder filterAggBuilder = new FilterAggregationBuilder("titles", new MatchAllQueryBuilder()).subAggregation(
            new RewrittenPipelineAggregationBuilder()
        );
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(filterAggBuilder);
        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), null, () -> 0L);
        AggregatorFactories.Builder rewritten = builder.rewrite(context);
        CountDownLatch latch = new CountDownLatch(1);
        context.executeAsyncActions(new ActionListener<>() {
            @Override
            public void onResponse(Void aVoid) {
                assertNotSame(builder, rewritten);
                Collection<AggregationBuilder> aggregatorFactories = rewritten.getAggregatorFactories();
                assertEquals(1, aggregatorFactories.size());
                FilterAggregationBuilder rewrittenFilterAggBuilder = (FilterAggregationBuilder) aggregatorFactories.iterator().next();
                PipelineAggregationBuilder rewrittenPipeline = rewrittenFilterAggBuilder.getPipelineAggregations().iterator().next();
                assertThat(((RewrittenPipelineAggregationBuilder) rewrittenPipeline).setOnRewrite.get(), equalTo("rewritten"));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        latch.await();
    }

    public void testRewriteAggregationAtTopLevel() throws Exception {
        FilterAggregationBuilder filterAggBuilder = new FilterAggregationBuilder("titles", new MatchAllQueryBuilder());
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(filterAggBuilder)
            .addPipelineAggregator(new RewrittenPipelineAggregationBuilder());
        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), null, () -> 0L);
        AggregatorFactories.Builder rewritten = builder.rewrite(context);
        CountDownLatch latch = new CountDownLatch(1);
        context.executeAsyncActions(new ActionListener<>() {
            @Override
            public void onResponse(Void aVoid) {
                assertNotSame(builder, rewritten);
                PipelineAggregationBuilder rewrittenPipeline = rewritten.getPipelineAggregatorFactories().iterator().next();
                assertThat(((RewrittenPipelineAggregationBuilder) rewrittenPipeline).setOnRewrite.get(), equalTo("rewritten"));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        latch.await();
    }

    public void testBuildPipelineTreeResolvesPipelineOrder() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
        builder.addPipelineAggregator(PipelineAggregatorBuilders.avgBucket("bar", "foo"));
        builder.addPipelineAggregator(PipelineAggregatorBuilders.avgBucket("foo", "real"));
        builder.addAggregator(AggregationBuilders.avg("real").field("target"));
        PipelineTree tree = builder.buildPipelineTree();
        assertThat(tree.aggregators().stream().map(PipelineAggregator::name).collect(toList()), equalTo(List.of("foo", "bar")));
    }

    public void testSupportsParallelCollection() {
        ToLongFunction<String> randomCardinality = name -> randomLongBetween(1, 200);
        {
            AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
            assertTrue(builder.supportsParallelCollection(randomCardinality));
            builder.addAggregator(new FilterAggregationBuilder("name", new MatchAllQueryBuilder()));
            assertTrue(builder.supportsParallelCollection(randomCardinality));
        }
        {
            AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
            builder.addAggregator(new CardinalityAggregationBuilder("cardinality"));
            assertTrue(builder.supportsParallelCollection(randomCardinality));
        }
        {
            AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
            builder.addAggregator(new NestedAggregationBuilder("nested", "path"));
            assertTrue(builder.supportsParallelCollection(randomCardinality));
        }
        {
            AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
            builder.addAggregator(new SignificantTermsAggregationBuilder("name"));
            assertFalse(builder.supportsParallelCollection(randomCardinality));
        }
        {
            AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
            builder.addAggregator(new FilterAggregationBuilder("terms", new MatchAllQueryBuilder()) {
                @Override
                public boolean isInSortOrderExecutionRequired() {
                    return true;
                }
            });
            assertFalse(builder.supportsParallelCollection(randomCardinality));
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private class RewrittenPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<RewrittenPipelineAggregationBuilder> {
        private final Supplier<String> setOnRewrite;

        RewrittenPipelineAggregationBuilder() {
            super("test", "rewritten", Strings.EMPTY_ARRAY);
            setOnRewrite = null;
        }

        RewrittenPipelineAggregationBuilder(Supplier<String> setOnRewrite) {
            super("test", "rewritten", Strings.EMPTY_ARRAY);
            this.setOnRewrite = setOnRewrite;
        }

        @Override
        public PipelineAggregationBuilder rewrite(QueryRewriteContext context) throws IOException {
            if (setOnRewrite != null) {
                return this;
            }
            SetOnce<String> loaded = new SetOnce<>();
            context.registerAsyncAction((client, listener) -> {
                loaded.set("rewritten");
                listener.onResponse(null);
            });
            return new RewrittenPipelineAggregationBuilder(loaded::get);
        }

        @Override
        public String getWriteableName() {
            return "rewritten";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.zero();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void validate(ValidationContext context) {
            throw new UnsupportedOperationException();
        }
    }
}
