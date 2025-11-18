/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.elasticsearch.core.ReleasableIterator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests {@link AbstractPageMappingToIteratorOperator} against a test
 * subclass that removes every other page.
 */
public class IteratorRemovePageTests extends OperatorTestCase {
    private static class IteratorRemovePage extends AbstractPageMappingToIteratorOperator {
        private static class Factory implements OperatorFactory {
            @Override
            public Operator get(DriverContext driverContext) {
                return new IteratorRemovePage();
            }

            @Override
            public String describe() {
                return "IteratorRemovePage[]";
            }
        }

        private boolean keep = true;

        @Override
        protected ReleasableIterator<Page> receive(Page page) {
            if (keep) {
                keep = false;
                return new ReleasableIterator<>() {
                    Page p = page;

                    @Override
                    public boolean hasNext() {
                        return p != null;
                    }

                    @Override
                    public Page next() {
                        Page ret = p;
                        p = null;
                        return ret;
                    }

                    @Override
                    public void close() {
                        if (p != null) {
                            p.releaseBlocks();
                        }
                    }
                };
            }
            keep = true;
            page.releaseBlocks();
            return new ReleasableIterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Page next() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String toString() {
            return "IteratorRemovePage[]";
        }
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomLong()));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize((input.size() + 1) / 2));
        for (int i = 0; i < input.size(); i += 2) {
            assertThat(input.get(i), equalTo(results.get(i / 2)));
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new IteratorRemovePage.Factory();
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("IteratorRemovePage[]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }
}
