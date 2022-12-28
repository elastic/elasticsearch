/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ExpressionIterable;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ExpressionIterable.Expression;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionIterableTests extends ESTestCase {

    public void testEmpty() {
        ExpressionIterable expressionIterable = new ExpressionIterable(
            getContextWithOptions(getExpandWildcardsIndicesOptions()),
            List.of()
        );
        assertThat(expressionIterable.iterator().hasNext(), is(false));
        assertThat(expressionIterable.hasWildcard(), is(false));
        expressionIterable = new ExpressionIterable(getContextWithOptions(getNoExpandWildcardsIndicesOptions()), List.of());
        assertThat(expressionIterable.iterator().hasNext(), is(false));
        assertThat(expressionIterable.hasWildcard(), is(false));
    }

    public void testExplicitSingleNameExpression() {
        for (IndicesOptions indicesOptions : List.of(getExpandWildcardsIndicesOptions(), getNoExpandWildcardsIndicesOptions())) {
            for (String expressionString : List.of("non_wildcard", "-non_exclusion")) {
                ExpressionIterable expressionIterable = new ExpressionIterable(
                    getContextWithOptions(indicesOptions),
                    List.of(expressionString)
                );
                assertThat(expressionIterable.hasWildcard(), is(false));
                if (randomBoolean()) {
                    expressionIterable = new ExpressionIterable(getContextWithOptions(indicesOptions), List.of(expressionString));
                }
                Iterator<Expression> expressionIterator = expressionIterable.iterator();
                assertThat(expressionIterator.hasNext(), is(true));
                if (randomBoolean()) {
                    expressionIterator = expressionIterable.iterator();
                }
                Expression expression = expressionIterator.next();
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.isWildcard(), is(false));
                assertThat(expression.toString(), is(expressionString));
                assertThat(expressionIterator.hasNext(), is(false));
            }
        }
    }

    public void testWildcardSingleExpression() {
        for (String wildcardTest : List.of("*", "a*", "*b", "a*b", "a-*b", "a*-b", "-*", "-a*", "-*b", "**", "*-*")) {
            ExpressionIterable expressionIterable = new ExpressionIterable(
                getContextWithOptions(getExpandWildcardsIndicesOptions()),
                List.of(wildcardTest)
            );
            assertThat(expressionIterable.hasWildcard(), is(true));
            if (randomBoolean()) {
                expressionIterable = new ExpressionIterable(
                    getContextWithOptions(getExpandWildcardsIndicesOptions()),
                    List.of(wildcardTest)
                );
            }
            Iterator<Expression> expressionIterator = expressionIterable.iterator();
            assertThat(expressionIterator.hasNext(), is(true));
            if (randomBoolean()) {
                expressionIterator = expressionIterable.iterator();
            }
            Expression expression = expressionIterator.next();
            assertThat(expression.isExclusion(), is(false));
            assertThat(expression.isWildcard(), is(true));
            assertThat(expression.toString(), is(wildcardTest));
            assertThat(expressionIterator.hasNext(), is(false));
        }
    }

    public void testWildcardLongerExpression() {
        List<String> onlyExplicits = randomList(7, () -> randomAlphaOfLengthBetween(0, 5));
        String wildcard = randomFrom("*", "*b", "-*", "*-", "c*", "a*b", "**");
        List<String> expressionList = new ArrayList<>(onlyExplicits.size() + 1);
        expressionList.addAll(randomSubsetOf(onlyExplicits));
        int wildcardPos = expressionList.size();
        expressionList.add(wildcard);
        for (String item : onlyExplicits) {
            if (expressionList.contains(item) == false) {
                expressionList.add(item);
            }
        }
        ExpressionIterable expressionIterable = new ExpressionIterable(
            getContextWithOptions(getExpandWildcardsIndicesOptions()),
            expressionList
        );
        assertThat(expressionIterable.hasWildcard(), is(true));
        if (randomBoolean()) {
            expressionIterable = new ExpressionIterable(getContextWithOptions(getExpandWildcardsIndicesOptions()), expressionList);
        }
        int i = 0;
        for (Expression expression : expressionIterable) {
            assertThat(expression.isExclusion(), is(false));
            if (i != wildcardPos) {
                assertThat(expression.isWildcard(), is(false));
            } else {
                assertThat(expression.isWildcard(), is(true));
            }
            assertThat(expression.toString(), is(expressionList.get(i++)));
        }
    }

    public void testWildcardsNoExclusionExpressions() {
        for (List<String> wildcardExpression : List.of(
            List.of("*"),
            List.of("a", "*"),
            List.of("-b", "*c"),
            List.of("-", "a", "c*"),
            List.of("*", "a*", "*b"),
            List.of("-*", "a", "b*")
        )) {
            ExpressionIterable expressionIterable = new ExpressionIterable(
                getContextWithOptions(getExpandWildcardsIndicesOptions()),
                wildcardExpression
            );
            assertThat(expressionIterable.hasWildcard(), is(true));
            if (randomBoolean()) {
                expressionIterable = new ExpressionIterable(getContextWithOptions(getExpandWildcardsIndicesOptions()), wildcardExpression);
            }
            int i = 0;
            for (Expression expression : expressionIterable) {
                assertThat(expression.isExclusion(), is(false));
                if (wildcardExpression.get(i).contains("*")) {
                    assertThat(expression.isWildcard(), is(true));
                } else {
                    assertThat(expression.isWildcard(), is(false));
                }
                assertThat(expression.toString(), is(wildcardExpression.get(i++)));
            }
        }
    }

    public void testWildcardExpressionNoExpandOptions() {
        for (List<String> wildcardExpression : List.of(
            List.of("*"),
            List.of("a", "*"),
            List.of("-b", "*c"),
            List.of("*d", "-"),
            List.of("*", "-*"),
            List.of("-", "a", "c*"),
            List.of("*", "a*", "*b")
        )) {
            ExpressionIterable expressionIterable = new ExpressionIterable(
                getContextWithOptions(getNoExpandWildcardsIndicesOptions()),
                wildcardExpression
            );
            assertThat(expressionIterable.hasWildcard(), is(false));
            if (randomBoolean()) {
                expressionIterable = new ExpressionIterable(
                    getContextWithOptions(getNoExpandWildcardsIndicesOptions()),
                    wildcardExpression
                );
            }
            int i = 0;
            for (Expression expression : expressionIterable) {
                assertThat(expression.isWildcard(), is(false));
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.toString(), is(wildcardExpression.get(i++)));
            }
        }
    }

    public void testSingleExclusionExpression() {
        String wildcard = randomFrom("*", "*b", "-*", "*-", "c*", "a*b", "**", "*-*");
        int wildcardPos = randomIntBetween(0, 3);
        String exclusion = randomFrom("-*", "-", "-c*", "-ab", "--");
        int exclusionPos = randomIntBetween(wildcardPos + 1, 7);
        List<String> exclusionExpression = new ArrayList<>();
        for (int i = 0; i < wildcardPos; i++) {
            exclusionExpression.add(randomAlphaOfLengthBetween(0, 5));
        }
        exclusionExpression.add(wildcard);
        for (int i = wildcardPos + 1; i < exclusionPos; i++) {
            exclusionExpression.add(randomAlphaOfLengthBetween(0, 5));
        }
        exclusionExpression.add(exclusion);
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            exclusionExpression.add(randomAlphaOfLengthBetween(0, 5));
        }
        ExpressionIterable expressionIterable = new ExpressionIterable(
            getContextWithOptions(getExpandWildcardsIndicesOptions()),
            exclusionExpression
        );
        if (randomBoolean()) {
            assertThat(expressionIterable.hasWildcard(), is(true));
        }
        int i = 0;
        for (Expression expression : expressionIterable) {
            if (i == wildcardPos) {
                assertThat(expression.isWildcard(), is(true));
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.toString(), is(exclusionExpression.get(i++)));
            } else if (i == exclusionPos) {
                assertThat(expression.isExclusion(), is(true));
                assertThat(expression.isWildcard(), is(exclusionExpression.get(i).contains("*")));
                assertThat(expression.toString(), is(exclusionExpression.get(i++).substring(1)));
            } else {
                assertThat(expression.isWildcard(), is(false));
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.toString(), is(exclusionExpression.get(i++)));
            }
        }
    }

    public void testExclusionsExpression() {
        for (Tuple<List<String>, List<Boolean>> exclusionExpression : List.of(
            new Tuple<>(List.of("-a", "*", "-a"), List.of(false, false, true)),
            new Tuple<>(List.of("-b*", "c", "-a"), List.of(false, false, true)),
            new Tuple<>(List.of("*d", "-", "*b"), List.of(false, true, false)),
            new Tuple<>(List.of("-", "--", "-*", "", "-*"), List.of(false, false, false, false, true)),
            new Tuple<>(List.of("*-", "-*", "a", "-b"), List.of(false, true, false, true)),
            new Tuple<>(List.of("a", "-b", "-*", "-b", "*", "-b"), List.of(false, false, false, true, false, true)),
            new Tuple<>(List.of("-a", "*d", "-a", "-*b", "-b", "--"), List.of(false, false, true, true, true, true))
        )) {
            ExpressionIterable expressionIterable = new ExpressionIterable(
                getContextWithOptions(getExpandWildcardsIndicesOptions()),
                exclusionExpression.v1()
            );
            if (randomBoolean()) {
                assertThat(expressionIterable.hasWildcard(), is(true));
            }
            int i = 0;
            for (Expression expression : expressionIterable) {
                boolean isExclusion = exclusionExpression.v2().get(i);
                assertThat(expression.isExclusion(), is(isExclusion));
                assertThat(expression.isWildcard(), is(exclusionExpression.v1().get(i).contains("*")));
                if (isExclusion) {
                    assertThat(expression.toString(), is(exclusionExpression.v1().get(i++).substring(1)));
                } else {
                    assertThat(expression.toString(), is(exclusionExpression.v1().get(i++)));
                }
            }
        }
    }

    private IndicesOptions getExpandWildcardsToOpenOnlyIndicesOptions() {
        return IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            true,
            false,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
    }

    private IndicesOptions getExpandWildcardsToCloseOnlyIndicesOptions() {
        return IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            false,
            true,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
    }

    private IndicesOptions getExpandWildcardsToOpenCloseIndicesOptions() {
        return IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            true,
            true,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
    }

    private IndicesOptions getExpandWildcardsIndicesOptions() {
        return ESTestCase.<Supplier<IndicesOptions>>randomFrom(
            this::getExpandWildcardsToOpenOnlyIndicesOptions,
            this::getExpandWildcardsToCloseOnlyIndicesOptions,
            this::getExpandWildcardsToOpenCloseIndicesOptions
        ).get();
    }

    private IndicesOptions getNoExpandWildcardsIndicesOptions() {
        return IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            false,
            false,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
    }

    private Context getContextWithOptions(IndicesOptions indicesOptions) {
        Context context = mock(Context.class);
        when(context.getOptions()).thenReturn(indicesOptions);
        return context;
    }
}
