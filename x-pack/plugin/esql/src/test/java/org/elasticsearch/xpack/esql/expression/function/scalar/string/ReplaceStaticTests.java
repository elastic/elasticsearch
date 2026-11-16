/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * These tests create rows that are 1MB in size. Test classes
 * which extend AbstractScalarFunctionTestCase rerun test cases with
 * many randomized inputs. Unfortunately, tests are run with
 * limited memory, and instantiating many copies of these
 * tests with large rows causes out of memory.
 */
public class ReplaceStaticTests extends ESTestCase {

    /**
     * Stack size for catastrophic-regex evaluation. Small enough that {@code Matcher.find()}
     * overflows independently of the test JVM {@code -Xss}, large enough for evaluator setup.
     */
    private static final long CATASTROPHIC_REGEX_STACK_SIZE = 256 * 1024;

    public void testLimit() {
        int textLength = (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10;
        String text = randomAlphaOfLength((int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10);
        String regex = "^(.+)$";

        // 10 times the original text + the remainder
        String extraString = "a".repeat((int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE % 10);
        assert textLength * 10 + extraString.length() == ScalarFunction.MAX_BYTES_REF_RESULT_SIZE;
        String newStr = "$0$0$0$0$0$0$0$0$0$0" + extraString;

        String result = process(text, regex, newStr);
        assertThat(result, equalTo(newStr.replaceAll("\\$\\d", text)));
    }

    /**
     * A catastrophic regex like {@code (a|aa)+$} against a long run of {@code a}s
     * used to throw {@link StackOverflowError} from {@code Matcher.find()}, which
     * is a fatal JVM error and killed the node. It must be converted to
     * {@link IllegalArgumentException} so the evaluator can warn and return null.
     * <p>
     * Evaluation runs on a child thread with a fixed stack so the overflow does not
     * depend on the test JVM's {@code -Xss}.
     */
    public void testCatastrophicRegexDoesNotThrowStackOverflowError() {
        String text = "a".repeat(10000);
        String regex = "(a|aa)+$";
        assertNull(processOnSmallStack(text, regex, "x"));
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: Pattern nesting is too deep to evaluate"
        );
    }

    /**
     * Same payload as {@link #testCatastrophicRegexDoesNotThrowStackOverflowError} but through
     * the constant-regex evaluator, which is the path taken by
     * {@code EVAL r = REPLACE(f, "(a|aa)+$", "x")}.
     */
    public void testCatastrophicConstantRegexDoesNotThrowStackOverflowError() {
        String text = "a".repeat(10000);
        String regex = "(a|aa)+$";
        assertNull(processConstantRegexOnSmallStack(text, regex, "x"));
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: Pattern nesting is too deep to evaluate"
        );
    }

    public void testInvalidConstantRegexWarnsAndReturnsNull() {
        PatternSyntaxException pse = expectThrows(PatternSyntaxException.class, () -> Pattern.compile("["));
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef("["), DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef("x"), DataType.KEYWORD)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef("a"))));
        ) {
            assertTrue(block.isNull(0));
        }
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: " + pse.getClass().getName() + ": " + pse.getMessage()
        );
    }

    public void testTooBig() {
        String textAndNewStr = randomAlphaOfLength((int) (ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10));
        String regex = ".";

        String result = process(textAndNewStr, regex, textAndNewStr);
        assertNull(result);
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: "
                + "Creating strings with more than ["
                + ScalarFunction.MAX_BYTES_REF_RESULT_SIZE
                + "] bytes is not supported"
        );
    }

    public void testTooBigWithGroups() {
        int textLength = (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE / 10;
        String text = randomAlphaOfLength(textLength);
        String regex = "(.+)";

        // 10 times the original text + the remainder + 1
        String extraString = "a".repeat(1 + (int) ScalarFunction.MAX_BYTES_REF_RESULT_SIZE % 10);
        assert textLength * 10 + extraString.length() == ScalarFunction.MAX_BYTES_REF_RESULT_SIZE + 1;
        String newStr = "$0$1$0$1$0$1$0$1$0$1" + extraString;

        String result = process(text, regex, newStr);
        assertNull(result);
        assertDriverWarnings(
            "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
            "Line -1:-1: java.lang.IllegalArgumentException: "
                + "Creating strings with more than ["
                + ScalarFunction.MAX_BYTES_REF_RESULT_SIZE
                + "] bytes is not supported"
        );
    }

    public String process(String text, String regex, String newStr) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    field("regex", DataType.KEYWORD),
                    field("newStr", DataType.KEYWORD)
                )
            ).get(driverContext());
            Block block = eval.eval(row(List.of(new BytesRef(text), new BytesRef(regex), new BytesRef(newStr))));
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    private String processOnSmallStack(String text, String regex, String newStr) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    field("regex", DataType.KEYWORD),
                    field("newStr", DataType.KEYWORD)
                )
            ).get(driverContext())
        ) {
            return evalOnSmallStack(eval, row(List.of(new BytesRef(text), new BytesRef(regex), new BytesRef(newStr))));
        }
    }

    private String processConstantRegexOnSmallStack(String text, String regex, String newStr) {
        try (
            var eval = AbstractScalarFunctionTestCase.evaluator(
                new Replace(
                    Source.EMPTY,
                    field("text", DataType.KEYWORD),
                    new Literal(Source.EMPTY, new BytesRef(regex), DataType.KEYWORD),
                    field("newStr", DataType.KEYWORD)
                )
            ).get(driverContext())
        ) {
            return evalOnSmallStack(eval, row(List.of(new BytesRef(text), new BytesRef(newStr))));
        }
    }

    private static String evalOnSmallStack(ExpressionEvaluator eval, Page page) {
        AtomicReference<String> result = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread thread = new Thread(Thread.currentThread().getThreadGroup(), () -> {
            try (Block block = eval.eval(page)) {
                result.set(block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString());
            } catch (Throwable t) {
                error.set(t);
            }
        }, "replace-catastrophic-regex", CATASTROPHIC_REGEX_STACK_SIZE);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for small-stack replace", e);
        }
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        return result.get();
    }

    /**
     * The following fields and methods were borrowed from AbstractScalarFunctionTestCase
     */
    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());
    private final List<DriverContext> driverContexts = Collections.synchronizedList(new ArrayList<>());

    /**
     * Asserts the warnings accumulated across every {@link DriverContext} this test built.
     * If you need the old style thread-local warnings from {@link HeaderWarning}, use
     * {@link #takeResponseWarnings}.
     */
    private void assertDriverWarnings(String... expected) {
        Set<String> collected = new LinkedHashSet<>();
        for (DriverContext ctx : driverContexts) {
            if (ctx.isFinished() == false) {
                ctx.finish();
            }
            collected.addAll(ctx.warnings());
        }
        assertThat(collected, containsInAnyOrder(expected));
    }

    private static Page row(List<Object> values) {
        return new Page(1, BlockUtils.fromListRow(TestBlockFactory.getNonBreakingInstance(), values));
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        DriverContext driverContext = new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
        driverContexts.add(driverContext);
        return driverContext;
    }

    @After
    public void allMemoryReleased() {
        for (CircuitBreaker breaker : breakers) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }
}
