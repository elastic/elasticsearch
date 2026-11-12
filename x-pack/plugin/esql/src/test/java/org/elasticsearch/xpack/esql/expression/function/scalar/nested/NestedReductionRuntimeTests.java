/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises row isolation and ownership on real compute state, including a deliberate partial-frame failure. */
public class NestedReductionRuntimeTests extends LuceneTestCase {
    @BeforeClass
    public static void logging() {
        LogConfigurator.configureESLogging();
    }

    public void testSameDocumentRowsAndLaterPagesHaveIndependentState() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            Page page = page(factory, "{\"items\":[{\"price\":3,\"quantity\":2},{\"price\":9}]}", "orders", 2);
            try (var result = evaluator.eval(page)) {
                assertEquals(2, result.getPositionCount());
                assertEquals(6L, BlockUtils.toJavaObject(result, 0));
                assertEquals(6L, BlockUtils.toJavaObject(result, 1));
            } finally {
                page.releaseBlocks();
            }
            page = page(factory, "{\"items\":[{\"price\":3,\"quantity\":2}]}", "orders", 1);
            try (var result = evaluator.eval(page)) {
                assertEquals(6L, BlockUtils.toJavaObject(result, 0));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testFailureBuildingSecondChildChannelReleasesFirstAndReservation() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            // Deliberate invalid input tests ownership; real mapped-source normalization is a separate loading concern.
            Page page = page(factory, "{\"items\":[{\"price\":3,\"quantity\":\"invalid long\"}]}", "orders", 1);
            try {
                assertThrows(NumberFormatException.class, () -> evaluator.eval(page));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testUnmappedIndexCannotUseSourceArrayAsMappedScope() {
        var factory = new BlockFactory(new NoopCircuitBreaker("nested-test"), BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            Page page = page(factory, "{\"items\":[{\"price\":999,\"quantity\":2}]}", "unmapped", 1);
            try (var result = evaluator.eval(page)) {
                assertNull(BlockUtils.toJavaObject(result, 0));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
    }

    public void testMissingSourceFailsInsteadOfLookingLikeAnEmptyCollection() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = new Page(1, factory.newConstantNullBlock(1), factory.newConstantBytesRefBlockWith(new BytesRef("orders"), 1));
            try {
                var error = assertThrows(IllegalArgumentException.class, () -> evaluator.eval(page));
                assertTrue(error.getMessage(), error.getMessage().contains("requires stored, unfiltered _source"));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testNumericStringsUseMapperCoercion() {
        var factory = new BlockFactory(new NoopCircuitBreaker("nested-test"), BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            Page page = page(
                factory,
                "{\"items\":[{\"price\":[\"3.5\",\"\",null],\"quantity\":2},{\"price\":\"\",\"quantity\":7}]}",
                "orders",
                1
            );
            try (var result = evaluator.eval(page)) {
                assertEquals(6L, BlockUtils.toJavaObject(result, 0));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
    }

    public void testOutOfRangeSourceNumberCannotWrapSilently() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            Page page = page(factory, "{\"items\":[{\"price\":9223372036854775808,\"quantity\":2}]}", "orders", 1);
            try {
                assertThrows(IllegalArgumentException.class, () -> evaluator.eval(page));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testCancellationInsideChildLoadingReleasesPartialFrame() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        var checks = new AtomicInteger();
        context.initializeEarlyTerminationChecker(() -> {
            if (checks.incrementAndGet() == 4) {
                throw new TaskCancelledException("deliberate cancellation after first child channel");
            }
        });
        try (var evaluator = sum(context)) {
            Page page = page(factory, "{\"items\":[{\"price\":3,\"quantity\":2}]}", "orders", 1);
            try {
                assertThrows(TaskCancelledException.class, () -> evaluator.eval(page));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testInvalidIndexMetadataCannotLookLikeAnUnmappedScope() {
        for (Object index : Arrays.asList(null, "", List.of("orders", "other"))) {
            assertMetadataFailure("{\"items\":[{\"price\":3,\"quantity\":2}]}", index, "_index metadata");
        }
    }

    public void testMultivaluedSourceCannotUseOnlyFirstValue() {
        assertMetadataFailure(List.of("{\"items\":[{\"price\":3,\"quantity\":2}]}", "{\"items\":[]}"), "orders", "_source metadata");
    }

    public void testNonObjectCollectionValuesCannotBeDroppedSilently() {
        assertMetadataFailure("{\"items\":[{\"price\":3,\"quantity\":2},42]}", "orders", "object");
    }

    public void testNestedSourceArraysRetainEveryObjectAndNullSlots() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = page(factory, "{\"items\":[null,[{\"price\":3,\"quantity\":2},null],[{\"price\":5,\"quantity\":2}]]}", "orders", 1);
            try (var result = evaluator.eval(page)) {
                assertEquals(16L, BlockUtils.toJavaObject(result, 0));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testUnmappedScopeSkipsUnavailableOrMalformedSource() {
        for (Object source : Arrays.asList(null, "invalid JSON", List.of("invalid", "JSON"))) {
            var breaker = new TrackingBreaker();
            var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
            var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
            try (var evaluator = sum(context)) {
                var page = new Page(1, metadataBlock(factory, source), metadataBlock(factory, "unmapped"));
                try (var result = evaluator.eval(page)) {
                    assertNull(BlockUtils.toJavaObject(result, 0));
                } finally {
                    page.releaseBlocks();
                }
            } finally {
                context.finish();
            }
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testCancellationDuringCollectionTraversalPrecedesChildAllocation() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        var checks = new AtomicInteger();
        context.initializeEarlyTerminationChecker(() -> {
            if (checks.incrementAndGet() == 2) {
                assertEquals("cancel during traversal, before building child channels", 0, breaker.blockAllocationsAfterFrame);
                throw new TaskCancelledException("deliberate collection traversal cancellation");
            }
        });
        try (var evaluator = sum(context)) {
            var page = page(factory, "{\"items\":[{\"price\":3,\"quantity\":2}]}", "orders", 1);
            try {
                assertThrows(TaskCancelledException.class, () -> evaluator.eval(page));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testAllocationFailureSweepReleasesAllState() {
        int allocations = exerciseAllocationFailure(0);
        assertTrue("exercise frame, child channels and result allocations", allocations > 4);
        for (int tripAt = 1; tripAt <= allocations; tripAt++) {
            exerciseAllocationFailure(tripAt);
        }
    }

    public void testCancellationDuringOneChildScalarArrayReleasesState() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        var checks = new AtomicInteger();
        context.initializeEarlyTerminationChecker(() -> {
            if (checks.incrementAndGet() == 4) {
                throw new TaskCancelledException("deliberate scalar-array traversal cancellation");
            }
        });
        try (var evaluator = sum(context)) {
            var page = page(factory, "{\"items\":[{\"price\":[[3],[5]],\"quantity\":2}]}", "orders", 1);
            try {
                assertThrows(TaskCancelledException.class, () -> evaluator.eval(page));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testCancellationDuringPredicateValidationReleasesState() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        var checks = new AtomicInteger();
        context.initializeEarlyTerminationChecker(() -> {
            if (checks.incrementAndGet() == 4) {
                throw new TaskCancelledException("deliberate predicate validation cancellation");
            }
        });
        var source = new ReferenceAttribute(Source.EMPTY, "source", DataType.SOURCE);
        var index = new ReferenceAttribute(Source.EMPTY, "index", DataType.KEYWORD);
        var active = new ReferenceAttribute(Source.EMPTY, "active", DataType.BOOLEAN);
        var expression = new NestedReduction(
            Source.EMPTY,
            List.of(source, index),
            "items",
            Set.of("orders"),
            active,
            List.of(active),
            List.of("active"),
            List.of(Set.of("orders"))
        );
        try (
            var evaluator = EvalMapper.toEvaluator(
                FoldContext.small(),
                expression,
                new Layout.Builder().append(source).append(index).build()
            ).get(context)
        ) {
            var page = page(factory, "{\"items\":[{\"active\":true}]}", "orders", 1);
            try {
                assertThrows(TaskCancelledException.class, () -> { try (var ignored = evaluator.eval(page)) {} });
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testChildBuildersAreBoundedAcrossRepeatedParents() {
        var breaker = new TrackingBreaker();
        var childBuilders = new AtomicInteger();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE) {
            @Override
            public LongBlock.Builder newLongBlockBuilder(int estimatedSize) {
                assertTrue("child page capacity " + estimatedSize, estimatedSize <= 1024);
                if (estimatedSize == 1024) {
                    childBuilders.incrementAndGet();
                }
                return super.newLongBlockBuilder(estimatedSize);
            }
        };
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = page(factory, largeSource(), "orders", 2);
            try (var result = evaluator.eval(page)) {
                assertEquals(12300L, BlockUtils.toJavaObject(result, 0));
                assertEquals(12300L, BlockUtils.toJavaObject(result, 1));
                assertTrue("both parents must load multiple child pages", childBuilders.get() >= 8);
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testCancellationInFinalChildPageReleasesEarlierState() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE) {
            @Override
            public LongBlock.Builder newLongBlockBuilder(int estimatedSize) {
                if (estimatedSize == 2) {
                    throw new TaskCancelledException("deliberate final child page cancellation");
                }
                return super.newLongBlockBuilder(estimatedSize);
            }
        };
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = page(factory, largeSource(), "orders", 1);
            try {
                assertThrows(TaskCancelledException.class, () -> { try (var ignored = evaluator.eval(page)) {} });
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testAllocationFailureAcrossChildPagesReleasesState() {
        int allocations = exerciseAllocationFailure(0, largeSource());
        for (int tripAt = 1; tripAt <= allocations; tripAt++) {
            exerciseAllocationFailure(tripAt, largeSource());
        }
    }

    public void testSourceParsingCanCancelBeforeMalformedTail() {
        var fields = new StringBuilder("{\"padding\":{");
        for (int i = 0; i < 2048; i++) {
            fields.append("\"k").append(i).append("\":0,");
        }
        fields.append("\"end\":0},\"items\":BROKEN}");
        for (String source : List.of("{\"padding\":[" + "0,".repeat(2048) + "0],\"items\":BROKEN}", fields.toString())) {
            var breaker = new TrackingBreaker();
            var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
            var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
            var checks = new AtomicInteger();
            context.initializeEarlyTerminationChecker(() -> {
                if (checks.incrementAndGet() == 2) {
                    assertEquals("cancel during decoding, before child allocation", 0, breaker.blockAllocationsAfterFrame);
                    throw new TaskCancelledException("deliberate source decoding cancellation");
                }
            });
            try (var evaluator = sum(context)) {
                var page = page(factory, source, "orders", 1);
                try {
                    // The malformed tail proves cancellation occurs before source decoding finishes.
                    assertThrows(TaskCancelledException.class, () -> { try (var ignored = evaluator.eval(page)) {} });
                } finally {
                    page.releaseBlocks();
                }
            } finally {
                context.finish();
            }
            assertEquals(2, checks.get());
            assertEquals(0L, breaker.getUsed());
        }
    }

    public void testMalformedApplicableSourceReleasesReservation() {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = page(factory, "{\"items\":[", "orders", 1);
            try {
                assertThrows(XContentParseException.class, () -> { try (var ignored = evaluator.eval(page)) {} });
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testSourceDecodingRetainsSupportedEncodings() throws IOException {
        for (var type : List.of(XContentType.JSON, XContentType.SMILE, XContentType.CBOR)) {
            var breaker = new TrackingBreaker();
            var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
            var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
            try (var builder = XContentFactory.contentBuilder(type); var evaluator = sum(context)) {
                builder.map(Map.of("items", List.of(Map.of("price", 3, "quantity", 2), Map.of("price", 5, "quantity", 4))));
                var page = new Page(
                    1,
                    factory.newConstantBytesRefBlockWith(BytesReference.bytes(builder).toBytesRef(), 1),
                    factory.newConstantBytesRefBlockWith(new BytesRef("orders"), 1)
                );
                try (var result = evaluator.eval(page)) {
                    assertEquals(type.toString(), 26L, BlockUtils.toJavaObject(result, 0));
                } finally {
                    page.releaseBlocks();
                }
            } finally {
                context.finish();
            }
            assertEquals(type.toString(), 0L, breaker.getUsed());
        }
    }

    public void testParsedSourceChargeSurvivesParserClose() {
        var breaker = new TrackingBreaker();
        var checkedChildren = new AtomicInteger();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE) {
            @Override
            public LongBlock.Builder newLongBlockBuilder(int estimatedSize) {
                if (estimatedSize == 2) {
                    assertTrue("parsed source must be checked before child loading", breaker.parsedChecks > 0);
                    assertTrue("parsed source remains charged after parser close", breaker.getUsed() >= breaker.parsedUsageAtCheck);
                    checkedChildren.incrementAndGet();
                }
                return super.newLongBlockBuilder(estimatedSize);
            }
        };
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = page(factory, "{\"items\":[{\"price\":3,\"quantity\":2},{\"price\":5,\"quantity\":4}]}", "orders", 1);
            long before = breaker.getUsed();
            try {
                try (var result = evaluator.eval(page)) {
                    assertEquals(26L, BlockUtils.toJavaObject(result, 0));
                }
                assertTrue("both child channels were checked", checkedChildren.get() >= 2);
                assertEquals("parent source and child state released before returning", before, breaker.getUsed());
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    public void testRealBreakerRejectsParsedSourceBeyondByteEstimate() {
        // Each expected trip logs a warning. Repeated iterations in one JVM would exceed the output cap.
        withoutBreakerWarnings(this::rejectParsedSourceBeyondByteEstimate);
    }

    private void rejectParsedSourceBeyondByteEstimate() {
        for (int tokens : List.of(2000, 100000)) {
            // Decoded source stays materialized. A large child multivalue is the same reservation as other source tokens.
            var shapes = tokens == 2000
                ? List.of("[]", "[{\"price\":3,\"quantity\":2}]", "[{\"price\":[" + "1,".repeat(tokens - 1) + "1]}]")
                : List.of("[]", "[{\"price\":3,\"quantity\":2}]");
            for (String items : shapes) {
                var settings = Settings.builder()
                    .put("indices.breaker.total.use_real_memory", false)
                    // A byte size here is logged as a deprecation on every construction.
                    .put("indices.breaker.total.limit", "95%")
                    .put("indices.breaker.request.limit", tokens == 2000 ? "160kb" : "6mb")
                    .put("indices.breaker.request.overhead", 1.0)
                    .build();
                var service = new HierarchyCircuitBreakerService(
                    CircuitBreakerMetrics.NOOP,
                    settings,
                    List.of(),
                    ClusterSettings.createBuiltInClusterSettings(settings)
                );
                var breaker = service.getBreaker(CircuitBreaker.REQUEST);
                var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
                var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
                try (var evaluator = sum(context)) {
                    var source = "{\"padding\":[" + "null,".repeat(tokens - 1) + "null],\"items\":" + items + "}";
                    var page = page(factory, source, "orders", 1);
                    try {
                        var error = assertThrows(CircuitBreakingException.class, () -> { try (var ignored = evaluator.eval(page)) {} });
                        assertTrue(error.getMessage(), error.getMessage().contains(ParsedContentReservation.LABEL));
                        assertEquals(1L, breaker.getTrippedCount());
                    } finally {
                        page.releaseBlocks();
                    }
                } finally {
                    context.finish();
                }
                assertEquals("no charge retained after parsing rejection", 0L, breaker.getUsed());
            }
        }
    }

    /**
     * Silence expected breaker warnings for one test, then put the previous levels back.
     * {@code null} means the logger was inheriting its level.
     */
    private static void withoutBreakerWarnings(Runnable body) {
        Logger common = LogManager.getLogger("org.elasticsearch.common.breaker");
        Logger indices = LogManager.getLogger("org.elasticsearch.indices.breaker");
        Level commonLevel = common.getLevel();
        Level indicesLevel = indices.getLevel();
        Loggers.setLevel(common, Level.ERROR);
        Loggers.setLevel(indices, Level.ERROR);
        try {
            body.run();
        } finally {
            Loggers.setLevel(common, commonLevel);
            Loggers.setLevel(indices, indicesLevel);
        }
    }

    private static String largeSource() {
        return "{\"items\":[" + "{\"price\":3,\"quantity\":2},".repeat(2049) + "{\"price\":3,\"quantity\":2}]}";
    }

    private static int exerciseAllocationFailure(int tripAt) {
        return exerciseAllocationFailure(tripAt, "{\"items\":[{\"price\":3,\"quantity\":2},{\"price\":5,\"quantity\":2}]}");
    }

    private static int exerciseAllocationFailure(int tripAt, String source) {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = page(factory, source, "orders", 2);
            breaker.observe(tripAt);
            try {
                if (tripAt == 0) {
                    try (var result = evaluator.eval(page)) {
                        assertEquals(source.equals(largeSource()) ? 12300L : 16L, BlockUtils.toJavaObject(result, 0));
                        assertEquals(source.equals(largeSource()) ? 12300L : 16L, BlockUtils.toJavaObject(result, 1));
                    }
                } else {
                    assertThrows(CircuitBreakingException.class, () -> {
                        try (var ignored = evaluator.eval(page)) {
                            // Close unexpected success too, so a failing assertion cannot leak its result.
                        }
                    });
                }
            } finally {
                breaker.observing = false;
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals("reservation balance after allocation failure " + tripAt, 0L, breaker.getUsed());
        return breaker.allocations;
    }

    private static void assertMetadataFailure(Object source, Object index, String reason) {
        var breaker = new TrackingBreaker();
        var factory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        var context = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, factory, null);
        try (var evaluator = sum(context)) {
            var page = new Page(1, metadataBlock(factory, source), metadataBlock(factory, index));
            try {
                var error = assertThrows(IllegalArgumentException.class, () -> { try (var ignored = evaluator.eval(page)) {} });
                assertTrue(error.getMessage(), error.getMessage().contains(reason));
            } finally {
                page.releaseBlocks();
            }
        } finally {
            context.finish();
        }
        assertEquals(0L, breaker.getUsed());
    }

    private static Block metadataBlock(BlockFactory factory, Object value) {
        try (var builder = factory.newBytesRefBlockBuilder(1)) {
            if (value == null) {
                builder.appendNull();
            } else if (value instanceof List<?> values) {
                builder.beginPositionEntry();
                for (Object item : values) {
                    builder.appendBytesRef(new BytesRef(item.toString()));
                }
                builder.endPositionEntry();
            } else {
                builder.appendBytesRef(new BytesRef(value.toString()));
            }
            return builder.build();
        }
    }

    private static ExpressionEvaluator sum(DriverContext context) {
        var source = new ReferenceAttribute(Source.EMPTY, "source", DataType.SOURCE);
        var index = new ReferenceAttribute(Source.EMPTY, "index", DataType.KEYWORD);
        var price = new ReferenceAttribute(Source.EMPTY, "price", DataType.LONG);
        var quantity = new ReferenceAttribute(Source.EMPTY, "quantity", DataType.LONG);
        var body = new Sum(Source.EMPTY, new Mul(Source.EMPTY, price, quantity)).forTransportVersion(TransportVersion.current());
        var expression = new NestedReduction(
            Source.EMPTY,
            List.of(source, index),
            "items",
            Set.of("orders"),
            body,
            List.of(price, quantity),
            List.of("price", "quantity"),
            List.of(Set.of("orders"), Set.of("orders"))
        );
        return EvalMapper.toEvaluator(FoldContext.small(), expression, new Layout.Builder().append(source).append(index).build())
            .get(context);
    }

    private static Page page(BlockFactory factory, String source, String index, int rows) {
        return new Page(
            rows,
            factory.newConstantBytesRefBlockWith(new BytesRef(source), rows),
            factory.newConstantBytesRefBlockWith(new BytesRef(index), rows)
        );
    }

    private static class TrackingBreaker extends NoopCircuitBreaker {
        private long used;
        private boolean observing;
        private int tripAt;
        private int allocations;
        private boolean frameReserved;
        private int blockAllocationsAfterFrame;
        private int parsedChecks;
        private long parsedUsageAtCheck;

        TrackingBreaker() {
            super("nested-test");
        }

        void observe(int tripAt) {
            observing = true;
            allocations = 0;
            this.tripAt = tripAt;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            if (observing && ++allocations == tripAt) {
                observing = false;
                // A rejected reservation, like the real breaker, leaves its net usage unchanged.
                throw new CircuitBreakingException("deliberate allocation failure " + tripAt, CircuitBreaker.Durability.TRANSIENT);
            }
            if (label.equals("nested_reduction_frame")) {
                frameReserved = true;
            } else if (frameReserved && label.equals(ParsedContentReservation.LABEL) == false) {
                blockAllocationsAfterFrame++;
            }
            used += bytes;
            if (label.equals(ParsedContentReservation.LABEL)) {
                parsedChecks++;
                parsedUsageAtCheck = used;
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }
    }
}
