/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.Context;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ExpressionList;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ExpressionList.Expression;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.ResolvedExpression;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.action.support.IndexComponentSelector.DATA;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExpressionListTests extends ESTestCase {

    public void testEmpty() {
        ExpressionList expressionList = new ExpressionList(getContextWithOptions(getExpandWildcardsIndicesOptions()), List.of());
        assertThat(expressionList.iterator().hasNext(), is(false));
        assertThat(expressionList.hasWildcard(), is(false));
        expressionList = new ExpressionList(getContextWithOptions(getNoExpandWildcardsIndicesOptions()), List.of());
        assertThat(expressionList.iterator().hasNext(), is(false));
        assertThat(expressionList.hasWildcard(), is(false));
    }

    public void testExplicitSingleNameExpression() {
        for (IndicesOptions indicesOptions : List.of(getExpandWildcardsIndicesOptions(), getNoExpandWildcardsIndicesOptions())) {
            for (String expressionString : List.of("non_wildcard", "-non_exclusion")) {
                ExpressionList expressionList = new ExpressionList(
                    getContextWithOptions(indicesOptions),
                    List.of(new ResolvedExpression(expressionString, DATA))
                );
                assertThat(expressionList.hasWildcard(), is(false));
                if (randomBoolean()) {
                    expressionList = new ExpressionList(
                        getContextWithOptions(indicesOptions),
                        List.of(new ResolvedExpression(expressionString, DATA))
                    );
                }
                Iterator<Expression> expressionIterator = expressionList.iterator();
                assertThat(expressionIterator.hasNext(), is(true));
                if (randomBoolean()) {
                    expressionIterator = expressionList.iterator();
                }
                Expression expression = expressionIterator.next();
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.isWildcard(), is(false));
                assertThat(expression.get(), is(expressionString));
                assertThat(expressionIterator.hasNext(), is(false));
            }
        }
    }

    public void testWildcardSingleExpression() {
        for (String wildcardTest : List.of("*", "a*", "*b", "a*b", "a-*b", "a*-b", "-*", "-a*", "-*b", "**", "*-*")) {
            ExpressionList expressionList = new ExpressionList(
                getContextWithOptions(getExpandWildcardsIndicesOptions()),
                List.of(new ResolvedExpression(wildcardTest, DATA))
            );
            assertThat(expressionList.hasWildcard(), is(true));
            if (randomBoolean()) {
                expressionList = new ExpressionList(
                    getContextWithOptions(getExpandWildcardsIndicesOptions()),
                    List.of(new ResolvedExpression(wildcardTest, DATA))
                );
            }
            Iterator<Expression> expressionIterator = expressionList.iterator();
            assertThat(expressionIterator.hasNext(), is(true));
            if (randomBoolean()) {
                expressionIterator = expressionList.iterator();
            }
            Expression expression = expressionIterator.next();
            assertThat(expression.isExclusion(), is(false));
            assertThat(expression.isWildcard(), is(true));
            assertThat(expression.get(), is(wildcardTest));
            assertThat(expressionIterator.hasNext(), is(false));
        }
    }

    public void testWildcardLongerExpression() {
        List<String> onlyExplicits = randomList(7, () -> randomAlphaOfLengthBetween(0, 5));
        String wildcard = randomFrom("*", "*b", "-*", "*-", "c*", "a*b", "**");
        List<ResolvedExpression> expressionList = new ArrayList<>(onlyExplicits.size() + 1);
        expressionList.addAll(randomSubsetOf(onlyExplicits).stream().map(rnd -> new ResolvedExpression(rnd, DATA)).toList());
        int wildcardPos = expressionList.size();
        expressionList.add(new ResolvedExpression(wildcard, DATA));
        for (String item : onlyExplicits) {
            if (expressionList.contains(new ResolvedExpression(item, DATA)) == false) {
                expressionList.add(new ResolvedExpression(item, DATA));
            }
        }
        ExpressionList expressionIterable = new ExpressionList(getContextWithOptions(getExpandWildcardsIndicesOptions()), expressionList);
        assertThat(expressionIterable.hasWildcard(), is(true));
        if (randomBoolean()) {
            expressionIterable = new ExpressionList(getContextWithOptions(getExpandWildcardsIndicesOptions()), expressionList);
        }
        int i = 0;
        for (Expression expression : expressionIterable) {
            assertThat(expression.isExclusion(), is(false));
            if (i != wildcardPos) {
                assertThat(expression.isWildcard(), is(false));
            } else {
                assertThat(expression.isWildcard(), is(true));
            }
            assertThat(expression.get(), is(expressionList.get(i++).resource()));
        }
    }

    public void testWildcardsNoExclusionExpressions() {
        for (List<ResolvedExpression> wildcardExpression : List.of(
            List.of(new ResolvedExpression("*", DATA)),
            List.of(new ResolvedExpression("a", DATA), new ResolvedExpression("*", DATA)),
            List.of(new ResolvedExpression("-b", DATA), new ResolvedExpression("*c", DATA)),
            List.of(new ResolvedExpression("-", DATA), new ResolvedExpression("a", DATA), new ResolvedExpression("c*", DATA)),
            List.of(new ResolvedExpression("*", DATA), new ResolvedExpression("a*", DATA), new ResolvedExpression("*b", DATA)),
            List.of(new ResolvedExpression("-*", DATA), new ResolvedExpression("a", DATA), new ResolvedExpression("b*", DATA))
        )) {
            ExpressionList expressionList = new ExpressionList(
                getContextWithOptions(getExpandWildcardsIndicesOptions()),
                wildcardExpression
            );
            assertThat(expressionList.hasWildcard(), is(true));
            if (randomBoolean()) {
                expressionList = new ExpressionList(getContextWithOptions(getExpandWildcardsIndicesOptions()), wildcardExpression);
            }
            int i = 0;
            for (Expression expression : expressionList) {
                assertThat(expression.isExclusion(), is(false));
                if (wildcardExpression.get(i).resource().contains("*")) {
                    assertThat(expression.isWildcard(), is(true));
                } else {
                    assertThat(expression.isWildcard(), is(false));
                }
                assertThat(expression.get(), is(wildcardExpression.get(i++).resource()));
            }
        }
    }

    public void testWildcardExpressionNoExpandOptions() {
        for (List<ResolvedExpression> wildcardExpression : List.of(
            List.of(new ResolvedExpression("*", DATA)),
            List.of(new ResolvedExpression("a", DATA), new ResolvedExpression("*", DATA)),
            List.of(new ResolvedExpression("-b", DATA), new ResolvedExpression("*c", DATA)),
            List.of(new ResolvedExpression("*d", DATA), new ResolvedExpression("-", DATA)),
            List.of(new ResolvedExpression("*", DATA), new ResolvedExpression("-*", DATA)),
            List.of(new ResolvedExpression("-", DATA), new ResolvedExpression("a", DATA), new ResolvedExpression("c*", DATA)),
            List.of(new ResolvedExpression("*", DATA), new ResolvedExpression("a*", DATA), new ResolvedExpression("*b", DATA))
        )) {
            ExpressionList expressionList = new ExpressionList(
                getContextWithOptions(getNoExpandWildcardsIndicesOptions()),
                wildcardExpression
            );
            assertThat(expressionList.hasWildcard(), is(false));
            if (randomBoolean()) {
                expressionList = new ExpressionList(getContextWithOptions(getNoExpandWildcardsIndicesOptions()), wildcardExpression);
            }
            int i = 0;
            for (Expression expression : expressionList) {
                assertThat(expression.isWildcard(), is(false));
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.get(), is(wildcardExpression.get(i++).resource()));
            }
        }
    }

    public void testSingleExclusionExpression() {
        String wildcard = randomFrom("*", "*b", "-*", "*-", "c*", "a*b", "**", "*-*");
        int wildcardPos = randomIntBetween(0, 3);
        String exclusion = randomFrom("-*", "-", "-c*", "-ab", "--");
        int exclusionPos = randomIntBetween(wildcardPos + 1, 7);
        List<ResolvedExpression> exclusionExpression = new ArrayList<>();
        for (int i = 0; i < wildcardPos; i++) {
            exclusionExpression.add(new ResolvedExpression(randomAlphaOfLengthBetween(0, 5), DATA));
        }
        exclusionExpression.add(new ResolvedExpression(wildcard, DATA));
        for (int i = wildcardPos + 1; i < exclusionPos; i++) {
            exclusionExpression.add(new ResolvedExpression(randomAlphaOfLengthBetween(0, 5), DATA));
        }
        exclusionExpression.add(new ResolvedExpression(exclusion, DATA));
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            exclusionExpression.add(new ResolvedExpression(randomAlphaOfLengthBetween(0, 5), DATA));
        }
        ExpressionList expressionList = new ExpressionList(getContextWithOptions(getExpandWildcardsIndicesOptions()), exclusionExpression);
        if (randomBoolean()) {
            assertThat(expressionList.hasWildcard(), is(true));
        }
        int i = 0;
        for (Expression expression : expressionList) {
            if (i == wildcardPos) {
                assertThat(expression.isWildcard(), is(true));
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.get(), is(exclusionExpression.get(i++).resource()));
            } else if (i == exclusionPos) {
                assertThat(expression.isExclusion(), is(true));
                assertThat(expression.isWildcard(), is(exclusionExpression.get(i).resource().contains("*")));
                assertThat(expression.get(), is(exclusionExpression.get(i++).resource().substring(1)));
            } else {
                assertThat(expression.isWildcard(), is(false));
                assertThat(expression.isExclusion(), is(false));
                assertThat(expression.get(), is(exclusionExpression.get(i++).resource()));
            }
        }
    }

    public void testExclusionsExpression() {
        for (Tuple<List<ResolvedExpression>, List<Boolean>> exclusionExpression : List.of(
            new Tuple<>(
                List.of(new ResolvedExpression("-a", DATA), new ResolvedExpression("*", DATA), new ResolvedExpression("-a", DATA)),
                List.of(false, false, true)
            ),
            new Tuple<>(
                List.of(new ResolvedExpression("-b*", DATA), new ResolvedExpression("c", DATA), new ResolvedExpression("-a", DATA)),
                List.of(false, false, true)
            ),
            new Tuple<>(
                List.of(new ResolvedExpression("*d", DATA), new ResolvedExpression("-", DATA), new ResolvedExpression("*b", DATA)),
                List.of(false, true, false)
            ),
            new Tuple<>(
                List.of(
                    new ResolvedExpression("-", DATA),
                    new ResolvedExpression("--", DATA),
                    new ResolvedExpression("-*", DATA),
                    new ResolvedExpression("", DATA),
                    new ResolvedExpression("-*", DATA)
                ),
                List.of(false, false, false, false, true)
            ),
            new Tuple<>(
                List.of(
                    new ResolvedExpression("*-", DATA),
                    new ResolvedExpression("-*", DATA),
                    new ResolvedExpression("a", DATA),
                    new ResolvedExpression("-b", DATA)
                ),
                List.of(false, true, false, true)
            ),
            new Tuple<>(
                List.of(
                    new ResolvedExpression("a", DATA),
                    new ResolvedExpression("-b", DATA),
                    new ResolvedExpression("-*", DATA),
                    new ResolvedExpression("-b", DATA),
                    new ResolvedExpression("*", DATA),
                    new ResolvedExpression("-b", DATA)
                ),
                List.of(false, false, false, true, false, true)
            ),
            new Tuple<>(
                List.of(
                    new ResolvedExpression("-a", DATA),
                    new ResolvedExpression("*d", DATA),
                    new ResolvedExpression("-a", DATA),
                    new ResolvedExpression("-*b", DATA),
                    new ResolvedExpression("-b", DATA),
                    new ResolvedExpression("--", DATA)
                ),
                List.of(false, false, true, true, true, true)
            )
        )) {
            ExpressionList expressionList = new ExpressionList(
                getContextWithOptions(getExpandWildcardsIndicesOptions()),
                exclusionExpression.v1()
            );
            if (randomBoolean()) {
                assertThat(expressionList.hasWildcard(), is(true));
            }
            int i = 0;
            for (Expression expression : expressionList) {
                boolean isExclusion = exclusionExpression.v2().get(i);
                assertThat(expression.isExclusion(), is(isExclusion));
                assertThat(expression.isWildcard(), is(exclusionExpression.v1().get(i).resource().contains("*")));
                if (isExclusion) {
                    assertThat(expression.get(), is(exclusionExpression.v1().get(i++).resource().substring(1)));
                } else {
                    assertThat(expression.get(), is(exclusionExpression.v1().get(i++).resource()));
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
