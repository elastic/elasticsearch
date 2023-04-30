/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * A factory that knows how to create an {@link PipelineAggregator} of a
 * specific type.
 */
public abstract class PipelineAggregationBuilder
    implements
        VersionedNamedWriteable,
        BaseAggregationBuilder,
        ToXContentFragment,
        Rewriteable<PipelineAggregationBuilder> {

    protected final String name;
    protected final String[] bucketsPaths;

    /**
     * Constructs a new pipeline aggregator factory.
     *
     * @param name
     *            The aggregation name
     */
    protected PipelineAggregationBuilder(String name, String[] bucketsPaths) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null: [" + name + "]");
        }
        if (bucketsPaths == null) {
            throw new IllegalArgumentException("[bucketsPaths] must not be null: [" + name + "]");
        }
        this.name = name;
        this.bucketsPaths = bucketsPaths;
    }

    /** Return this aggregation's name. */
    public String getName() {
        return name;
    }

    /** Return the consumed buckets paths. */
    public final String[] getBucketsPaths() {
        return bucketsPaths;
    }

    /**
     * Makes sure this builder is properly configured.
     */
    protected abstract void validate(ValidationContext context);

    public abstract static class ValidationContext {
        /**
         * Build the context for the root of the aggregation tree.
         */
        public static ValidationContext forTreeRoot(
            Collection<AggregationBuilder> siblingAggregations,
            Collection<PipelineAggregationBuilder> siblingPipelineAggregations,
            ActionRequestValidationException validationFailuresSoFar
        ) {
            return new ForTreeRoot(siblingAggregations, siblingPipelineAggregations, validationFailuresSoFar);
        }

        /**
         * Build the context for a node inside the aggregation tree.
         */
        public static ValidationContext forInsideTree(AggregationBuilder parent, ActionRequestValidationException validationFailuresSoFar) {
            return new ForInsideTree(parent, validationFailuresSoFar);
        }

        private ActionRequestValidationException e;

        private ValidationContext(ActionRequestValidationException validationFailuresSoFar) {
            this.e = validationFailuresSoFar;
        }

        private static class ForTreeRoot extends ValidationContext {
            private final Collection<AggregationBuilder> siblingAggregations;
            private final Collection<PipelineAggregationBuilder> siblingPipelineAggregations;

            ForTreeRoot(
                Collection<AggregationBuilder> siblingAggregations,
                Collection<PipelineAggregationBuilder> siblingPipelineAggregations,
                ActionRequestValidationException validationFailuresSoFar
            ) {
                super(validationFailuresSoFar);
                this.siblingAggregations = Objects.requireNonNull(siblingAggregations);
                this.siblingPipelineAggregations = Objects.requireNonNull(siblingPipelineAggregations);
            }

            @Override
            public Collection<AggregationBuilder> getSiblingAggregations() {
                return siblingAggregations;
            }

            @Override
            public Collection<PipelineAggregationBuilder> getSiblingPipelineAggregations() {
                return siblingPipelineAggregations;
            }

            @Override
            public void validateHasParent(String type, String name) {
                addValidationError(type + " aggregation [" + name + "] must be declared inside of another aggregation");
            }

            @Override
            public void validateParentAggSequentiallyOrdered(String type, String name) {
                noParentCantBeOrdered(type, name);
            }

            @Override
            public void validateParentAggSequentiallyOrderedWithoutSkips(String type, String name) {
                noParentCantBeOrdered(type, name);
            }

            private void noParentCantBeOrdered(String type, String name) {
                addValidationError(
                    type
                        + " aggregation ["
                        + name
                        + "] must have a histogram, date_histogram or auto_date_histogram as parent but doesn't have a parent"
                );
            }
        }

        private static class ForInsideTree extends ValidationContext {
            private final AggregationBuilder parent;

            ForInsideTree(AggregationBuilder parent, ActionRequestValidationException validationFailuresSoFar) {
                super(validationFailuresSoFar);
                this.parent = Objects.requireNonNull(parent);
            }

            @Override
            public Collection<AggregationBuilder> getSiblingAggregations() {
                return parent.getSubAggregations();
            }

            @Override
            public Collection<PipelineAggregationBuilder> getSiblingPipelineAggregations() {
                return parent.getPipelineAggregations();
            }

            @Override
            public void validateHasParent(String type, String name) {
                // There is a parent inside the tree.
            }

            @Override
            public void validateParentAggSequentiallyOrdered(String type, String name) {
                parent.validateSequentiallyOrdered(type, name, this::addValidationError);
            }

            @Override
            public void validateParentAggSequentiallyOrderedWithoutSkips(String type, String name) {
                parent.validateSequentiallyOrderedWithoutGaps(type, name, this::addValidationError);
            }
        }

        /**
         * Aggregations that are siblings to the aggregation being validated.
         */
        public abstract Collection<AggregationBuilder> getSiblingAggregations();

        /**
         * Pipeline aggregations that are siblings to the aggregation being validated.
         */
        public abstract Collection<PipelineAggregationBuilder> getSiblingPipelineAggregations();

        /**
         * Add a validation error to this context. All validation errors
         * are accumulated in a list and, if there are any, the request
         * is not executed and the entire list is returned as the error
         * response.
         */
        public void addValidationError(String error) {
            e = ValidateActions.addValidationError(error, e);
        }

        /**
         * Add a validation error about the {@code buckets_path}.
         */
        public void addBucketPathValidationError(String error) {
            addValidationError(PipelineAggregator.Parser.BUCKETS_PATH.getPreferredName() + ' ' + error);
        }

        /**
         * Validates that there <strong>is</strong> a parent aggregation.
         */
        public abstract void validateHasParent(String type, String name);

        /**
         * Validates that the parent is sequentially ordered.
         */
        public abstract void validateParentAggSequentiallyOrdered(String type, String name);

        /**
         * Validates that the parent is sequentially ordered and doesn't have any gps.
         */
        public abstract void validateParentAggSequentiallyOrderedWithoutSkips(String type, String name);

        /**
         * The validation exception, if there is one. It'll be {@code null}
         * if the context wasn't provided with any exception on creation
         * and none were added.
         */
        public ActionRequestValidationException getValidationException() {
            return e;
        }
    }

    /**
     * Creates the pipeline aggregator
     *
     * @return The created aggregator
     */
    protected abstract PipelineAggregator create();

    /** Associate metadata with this {@link PipelineAggregationBuilder}. */
    @Override
    public abstract PipelineAggregationBuilder setMetadata(Map<String, Object> metadata);

    @Override
    public PipelineAggregationBuilder subAggregations(Builder subFactories) {
        throw new IllegalArgumentException("Aggregation [" + name + "] cannot define sub-aggregations");
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    /**
     * {@inheritDoc}
     * <p>
     * The default implementation return the same instance. It should be
     * overridden by aggregations that must load data before they can be run,
     * particularly if that load must by asynchronous.
     */
    @Override
    public PipelineAggregationBuilder rewrite(QueryRewriteContext context) throws IOException {
        return this;
    }
}
