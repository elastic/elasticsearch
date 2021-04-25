/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.AsyncSupplier;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class IndexAbstractionResolver {

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public IndexAbstractionResolver(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public List<String> resolveIndexAbstractions(String[] indices, IndicesOptions indicesOptions, Metadata metadata,
                                                 boolean includeDataStreams) {
        return resolveIndexAbstractions(Arrays.asList(indices), indicesOptions, metadata, includeDataStreams);
    }

    public List<String> resolveIndexAbstractions(Iterable<String> indices, IndicesOptions indicesOptions, Metadata metadata,
                                                 boolean includeDataStreams) {
        final boolean replaceWildcards = indicesOptions.expandWildcardsOpen() || indicesOptions.expandWildcardsClosed();
        Set<String> availableIndexAbstractions = metadata.getIndicesLookup().keySet();
        return resolveIndexAbstractions(indices, indicesOptions, metadata, availableIndexAbstractions, replaceWildcards,
            includeDataStreams);
    }

    public List<String> resolveIndexAbstractions(Iterable<String> indices, IndicesOptions indicesOptions, Metadata metadata,
                                                 Collection<String> availableIndexAbstractions, boolean replaceWildcards,
                                                 boolean includeDataStreams) {
        PlainActionFuture<List<String>> future = PlainActionFuture.newFuture();
        // if the supplier is not async, the method is not async, hence get is non-blocking
        resolveIndexAbstractions(indices, indicesOptions, metadata, listener -> listener.onResponse(availableIndexAbstractions),
                replaceWildcards,
                includeDataStreams,
                future);
        return FutureUtils.get(future, 0, TimeUnit.MILLISECONDS);
    }

    public void resolveIndexAbstractions(Iterable<String> indices, IndicesOptions indicesOptions, Metadata metadata,
                                         AsyncSupplier<Collection<String>> availableIndexAbstractionsSupplier, boolean replaceWildcards,
                                         boolean includeDataStreams, ActionListener<List<String>> listener) {
        final Iterator<String> indicesIterator = indices.iterator();
        final Runnable evalItems = new ActionRunnable(listener) {

            final List<String> finalIndices = new ArrayList<>();
            final AtomicBoolean wildcardSeen = new AtomicBoolean(false);

            @Override
            public void doRun() {
                if (false == indicesIterator.hasNext()) {
                    listener.onResponse(finalIndices);
                } else {
                    final String index = indicesIterator.next();
                    final AtomicReference<String> indexAbstraction;
                    final boolean minus;
                    if (index.charAt(0) == '-' && wildcardSeen.get()) {
                        indexAbstraction = new AtomicReference<>(index.substring(1));
                        minus = true;
                    } else {
                        indexAbstraction = new AtomicReference<>(index);
                        minus = false;
                    }

                    // we always need to check for date math expressions
                    final String dateMathName = indexNameExpressionResolver.resolveDateMathExpression(indexAbstraction.get());
                    final StepListener<Void> evaluateDateMathStep = new StepListener<>();
                    if (dateMathName != indexAbstraction.get()) {
                        assert dateMathName.equals(indexAbstraction.get()) == false;
                        if (replaceWildcards && Regex.isSimpleMatchPattern(dateMathName)) {
                            // this is a date math and a wildcard
                            indexAbstraction.set(dateMathName);
                            // continue
                            evaluateDateMathStep.onResponse(null);
                        } else {
                            availableIndexAbstractionsSupplier.getAsync(ActionListener.wrap(availableIndexAbstractions -> {
                                if (availableIndexAbstractions.contains(dateMathName) &&
                                        isIndexVisible(indexAbstraction.get(), dateMathName, indicesOptions, metadata, includeDataStreams, true)) {
                                    if (minus) {
                                        finalIndices.remove(dateMathName);
                                    } else {
                                        finalIndices.add(dateMathName);
                                    }
                                } else {
                                    if (indicesOptions.ignoreUnavailable() == false) {
                                        listener.onFailure(new IndexNotFoundException(dateMathName));
                                        return;
                                    }
                                }
                                evaluateDateMathStep.onResponse(null);
                            }, listener::onFailure));
                        }
                    } else {
                        evaluateDateMathStep.onResponse(null);
                    }

                    evaluateDateMathStep.whenComplete(anotherVoid -> {
                        if (replaceWildcards && Regex.isSimpleMatchPattern(indexAbstraction.get())) {
                            wildcardSeen.set(true);
                            availableIndexAbstractionsSupplier.getAsync(ActionListener.wrap(availableIndexAbstractions -> {
                                Set<String> resolvedIndices = new HashSet<>();
                                for (String authorizedIndex : availableIndexAbstractions) {
                                    if (Regex.simpleMatch(indexAbstraction.get(), authorizedIndex) &&
                                            isIndexVisible(indexAbstraction.get(), authorizedIndex, indicesOptions, metadata, includeDataStreams)) {
                                        resolvedIndices.add(authorizedIndex);
                                    }
                                }
                                if (resolvedIndices.isEmpty()) {
                                    //es core honours allow_no_indices for each wildcard expression, we do the same here by throwing index not found.
                                    if (indicesOptions.allowNoIndices() == false) {
                                        listener.onFailure(new IndexNotFoundException(indexAbstraction.get()));
                                        return;
                                    }
                                } else {
                                    if (minus) {
                                        finalIndices.removeAll(resolvedIndices);
                                    } else {
                                        finalIndices.addAll(resolvedIndices);
                                    }
                                }
                                // next expression item
                                this.doRun();
                            }, listener::onFailure));
                        } else if (dateMathName.equals(indexAbstraction.get())) {
                            if (minus) {
                                finalIndices.remove(indexAbstraction.get());
                            } else {
                                finalIndices.add(indexAbstraction.get());
                            }
                            // next expression item
                            this.doRun();
                        } else {
                            // next expression item
                            this.doRun();
                        }
                    }, listener::onFailure);
                }
            }
        };
        evalItems.run();
    }

    public static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata,
                                         boolean includeDataStreams) {
        return isIndexVisible(expression, index, indicesOptions, metadata, includeDataStreams, false);
    }

    public static boolean isIndexVisible(String expression, String index, IndicesOptions indicesOptions, Metadata metadata,
                                          boolean includeDataStreams, boolean dateMathExpression) {
        IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
        if (indexAbstraction == null) {
            throw new IllegalStateException("could not resolve index abstraction [" + index + "]");
        }
        final boolean isHidden = indexAbstraction.isHidden();
        boolean isVisible = isHidden == false || indicesOptions.expandWildcardsHidden() || isVisibleDueToImplicitHidden(expression, index);
        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS) {
            //it's an alias, ignore expandWildcardsOpen and expandWildcardsClosed.
            //complicated to support those options with aliases pointing to multiple indices...
            return isVisible && indicesOptions.ignoreAliases() == false;
        }
        if (indexAbstraction.getType() == IndexAbstraction.Type.DATA_STREAM) {
            return isVisible && includeDataStreams;
        }
        assert indexAbstraction.getIndices().size() == 1 : "concrete index must point to a single index";
        // since it is a date math expression, we consider the index visible regardless of open/closed/hidden as the user is using
        // date math to explicitly reference the index
        if (dateMathExpression) {
            assert IndexMetadata.State.values().length == 2 : "a new IndexMetadata.State value may need to be handled!";
            return true;
        }
        if (isVisible == false) {
            return false;
        }

        IndexMetadata indexMetadata = indexAbstraction.getIndices().get(0);
        if (indexMetadata.getState() == IndexMetadata.State.CLOSE && indicesOptions.expandWildcardsClosed()) {
            return true;
        }
        if (indexMetadata.getState() == IndexMetadata.State.OPEN && indicesOptions.expandWildcardsOpen()) {
            return true;
        }
        return false;
    }

    private static boolean isVisibleDueToImplicitHidden(String expression, String index) {
        return index.startsWith(".") && expression.startsWith(".") && Regex.isSimpleMatchPattern(expression);
    }
}
