/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.common.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterService;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Validation of source indexes and destination index in different situations (preview, put)
 */
public final class SourceDestValidator {

    // messages
    public static final String SOURCE_INDEX_MISSING = "Source index [{0}] does not exist";
    public static final String DEST_IN_SOURCE = "Destination index [{0}] is included in source expression [{1}]";
    public static final String NEEDS_REMOTE_CLUSTER_SEARCH = "Source index is configured with a remote index pattern(s) [{0}]"
        + " but the current node [{1}] is not allowed to connect to remote clusters."
        + " Please enable cluster.remote.connect for all data nodes.";
    public static final String ERROR_REMOTE_CLUSTER_SEARCH = "Error during resolving remote source, error: {0}";
    public static final String UNKNOWN_REMOTE_CLUSTER_LICENSE = "Error during license check ({0}) for remote cluster "
        + "alias(es) {1}, error: {2}";
    public static final String FEATURE_NOT_LICENSED_REMOTE_CLUSTER_LICENSE = "License check failed for remote cluster "
        + "alias [{0}], at least a [{1}] license is required, found license [{2}]";
    public static final String REMOTE_CLUSTER_LICENSE_INACTIVE = "License check failed for remote cluster "
        + "alias [{0}], license is not active";
    public static final String TIMEOUT_CHECK_REMOTE_CLUSTER_LICENSE = "Timeout during license check ({0}) for remote cluster "
        + "alias(es) [{1}]";

    static class Context {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final RemoteClusterService remoteClusterService;
        private final RemoteClusterLicenseChecker remoteClusterLicenseChecker;
        private final String[] source;
        private final String dest;
        private final String nodeName;
        private final String license;

        private ValidationException validationException = null;
        private Set<String> resolvedSource = null;
        private Set<String> resolvedRemoteSource = null;
        private String resolvedDest = null;

        Context(
            final ClusterState state,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final RemoteClusterService remoteClusterService,
            final RemoteClusterLicenseChecker remoteClusterLicenseChecker,
            final String[] source,
            final String dest,
            final String nodeName,
            final String license
        ) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
            this.remoteClusterService = remoteClusterService;
            this.remoteClusterLicenseChecker = remoteClusterLicenseChecker;
            this.source = source;
            this.dest = dest;
            this.nodeName = nodeName;
            this.license = license;
        }

        public ClusterState getState() {
            return state;
        }

        public RemoteClusterService getRemoteClusterService() {
            return remoteClusterService;
        }

        public RemoteClusterLicenseChecker getRemoteClusterLicenseChecker() {
            return remoteClusterLicenseChecker;
        }

        public IndexNameExpressionResolver getIndexNameExpressionResolver() {
            return indexNameExpressionResolver;
        }

        public boolean isRemoteSearchEnabled() {
            return remoteClusterLicenseChecker != null;
        }

        public String[] getSource() {
            return source;
        }

        public String getDest() {
            return dest;
        }

        public String getNodeName() {
            return nodeName;
        }

        public String getLicense() {
            return license;
        }

        public Set<String> resolveSource() {
            if (resolvedSource == null) {
                resolveLocalAndRemoteSource();
            }

            return resolvedSource;
        }

        public Set<String> resolveRemoteSource() {
            if (resolvedRemoteSource == null) {
                resolveLocalAndRemoteSource();
            }

            return resolvedRemoteSource;
        }

        public String resolveDest() {
            if (resolvedDest == null) {
                try {
                    Index singleWriteIndex = indexNameExpressionResolver.concreteWriteIndex(
                        state,
                        IndicesOptions.lenientExpandOpen(),
                        dest,
                        true
                    );
                    if (singleWriteIndex != null) {
                        resolvedDest = singleWriteIndex.getName();
                    } else {
                        resolvedDest = dest;
                    }
                } catch (IllegalArgumentException e) {
                    // stop here as we can not return a single dest index
                    addValidationError(e.getMessage(), false);
                }
            }

            return resolvedDest;
        }

        public ValidationException addValidationError(String error, boolean continueValidation, Object... args) {
            if (validationException == null) {
                validationException = new ValidationException();
            }

            validationException.addValidationError(new MessageFormat(error, Locale.ROOT).format(args));
            if (continueValidation == false) {
                throw validationException;
            }

            return validationException;
        }

        public ValidationException getValidationException() {
            return validationException;
        }

        // convenience method to make testing easier
        public Set<String> getRegisteredRemoteClusterNames() {
            return remoteClusterService.getRegisteredRemoteClusterNames();
        }

        private void resolveLocalAndRemoteSource() {
            resolvedSource = new LinkedHashSet<>(Arrays.asList(source));
            resolvedRemoteSource = new LinkedHashSet<>(RemoteClusterLicenseChecker.remoteIndices(resolvedSource));
            resolvedSource.removeAll(resolvedRemoteSource);

            // special case: if indexNameExpressionResolver gets an empty list it treats it as _all
            if (resolvedSource.isEmpty() == false) {
                resolvedSource = new LinkedHashSet<>(
                    Arrays.asList(
                        indexNameExpressionResolver.concreteIndexNames(
                            state,
                            DEFAULT_INDICES_OPTIONS_FOR_VALIDATION,
                            resolvedSource.toArray(new String[0])
                        )
                    )
                );
            }
        }
    }

    interface SourceDestValidation {
        boolean isDeferrable();

        void validate(Context context);
    }

    // note: this is equivalent to the default for search requests
    private static final IndicesOptions DEFAULT_INDICES_OPTIONS_FOR_VALIDATION = IndicesOptions
        .strictExpandOpenAndForbidClosedIgnoreThrottled();

    private static final SourceDestValidation SOURCE_MISSING_VALIDATION = new SourceMissingValidation();
    private static final SourceDestValidation REMOTE_SOURCE_VALIDATION = new RemoteSourceEnabledAndRemoteLicenseValidation();

    private static final List<SourceDestValidation> PREVIEW_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION,
        REMOTE_SOURCE_VALIDATION
    );

    private static final List<SourceDestValidation> START_AND_PUT_VALIDATIONS = Arrays.asList(
        SOURCE_MISSING_VALIDATION,
        REMOTE_SOURCE_VALIDATION,
        new DestinationInSourceValidation(),
        new DestinationSingleIndexValidation()
    );

    /**
     * Validates source and dest indices for preview.
     *
     * @param clusterState The current ClusterState
     * @param indexNameExpressionResolver A valid IndexNameExpressionResolver object
     * @param remoteClusterService A valid RemoteClusterService object
     * @param remoteClusterLicenseChecker A RemoteClusterLicenseChecker or null if CCS is disabled
     * @param source an array of source indices
     * @param dest destination index
     * @param nodeName the name of this node
     * @param license the license of the feature validated for
     * @return ValidationException instance containing all validation errors or null if validation found no problems.
     */
    public static ValidationException validateForPreview(
        ClusterState clusterState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        RemoteClusterLicenseChecker remoteClusterLicenseChecker,
        String[] source,
        String dest,
        String nodeName,
        String license
    ) {
        Context context = new Context(
            clusterState,
            indexNameExpressionResolver,
            remoteClusterService,
            remoteClusterLicenseChecker,
            source,
            dest,
            nodeName,
            license
        );

        // validation exception might gets thrown if validation stops early
        try {
            for (SourceDestValidation validation : PREVIEW_VALIDATIONS) {
                validation.validate(context);
            }
        } catch (ValidationException e) {}

        return context.getValidationException();
    }

    /**
     * Validates source and destination indices for put, start and update.
     * Assumes that name checks have been made at the REST layer.
     *
     * @param clusterState The current ClusterState
     * @param indexNameExpressionResolver A valid IndexNameExpressionResolver object
     * @param remoteClusterService A valid RemoteClusterService object
     * @param remoteClusterLicenseChecker A RemoteClusterLicenseChecker or null if CCS is disabled
     * @param source an array of source indexes
     * @param dest destination index
     * @param nodeName the name of this node
     * @param license the license of the feature validated for
     * @param shouldDefer whether to defer certain validations, used for put while source indices are not created yet
     * @return ValidationException instance containing all validation errors or null if validation found no problems.
     */

    public static ValidationException validate(
        ClusterState clusterState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        RemoteClusterLicenseChecker remoteClusterLicenseChecker,
        String[] source,
        String dest,
        String nodeName,
        String license,
        boolean shouldDefer
    ) {
        Context context = new Context(
            clusterState,
            indexNameExpressionResolver,
            remoteClusterService,
            remoteClusterLicenseChecker,
            source,
            dest,
            nodeName,
            license
        );

        // validation exception might gets thrown if validation stops early
        try {
            for (SourceDestValidation validation : START_AND_PUT_VALIDATIONS) {
                if (shouldDefer && validation.isDeferrable()) {
                    continue;
                }
                validation.validate(context);
            }

        } catch (ValidationException e) {}

        return context.getValidationException();
    }

    static class SourceMissingValidation implements SourceDestValidation {

        @Override
        public boolean isDeferrable() {
            return true;
        }

        @Override
        public void validate(Context context) {
            // boolean remoteIndexesEmpty = context.isRemoteSearchEnabled() ? context.resolveRemoteSource().isEmpty() : true;

            try {
                // non-trivia: if source contains a wildcard index, which does not resolve to a concrete index
                // the resolved indices might be empty, but we can check if source contained something, this works because
                // of no wildcard index is involved the resolve would have thrown an exception
                if (context.resolveSource().isEmpty() && context.resolveRemoteSource().isEmpty() && context.getSource().length == 0) {
                    context.addValidationError(SOURCE_INDEX_MISSING, true, Strings.arrayToCommaDelimitedString(context.getSource()));
                }
            } catch (IndexNotFoundException e) {
                context.addValidationError(e.getMessage(), true);
            }
        }
    }

    static class RemoteSourceEnabledAndRemoteLicenseValidation implements SourceDestValidation {
        @Override
        public boolean isDeferrable() {
            return true;
        }

        @Override
        public void validate(Context context) {
            if (context.resolveRemoteSource().isEmpty()) {
                return;
            }

            List<String> remoteIndices = new ArrayList<>(context.resolveRemoteSource());
            // we can only check this node at the moment, clusters with mixed CCS enabled/disabled nodes are not supported, see gh#TBD
            if (context.isRemoteSearchEnabled() == false) {
                context.addValidationError(NEEDS_REMOTE_CLUSTER_SEARCH, true, context.resolveRemoteSource(), context.getNodeName());
                return;
            }

            // this can throw
            List<String> remoteAliases;
            try {
                remoteAliases = RemoteClusterLicenseChecker.remoteClusterAliases(context.getRegisteredRemoteClusterNames(), remoteIndices);
            } catch (NoSuchRemoteClusterException e) {
                context.addValidationError(e.getMessage(), true);
                return;
            } catch (Exception e) {
                context.addValidationError(ERROR_REMOTE_CLUSTER_SEARCH, true, e.getMessage());
                return;
            }

            CountDownLatch latch = new CountDownLatch(1);

            context.getRemoteClusterLicenseChecker()
                .checkRemoteClusterLicenses(remoteAliases, new LatchedActionListener<>(ActionListener.wrap(response -> {
                    if (response.isSuccess() == false) {
                        if (response.remoteClusterLicenseInfo().licenseInfo().getStatus() != LicenseStatus.ACTIVE) {
                            context.addValidationError(
                                REMOTE_CLUSTER_LICENSE_INACTIVE,
                                true,
                                response.remoteClusterLicenseInfo().clusterAlias()
                            );
                        } else {
                            context.addValidationError(
                                FEATURE_NOT_LICENSED_REMOTE_CLUSTER_LICENSE,
                                true,
                                response.remoteClusterLicenseInfo().clusterAlias(),
                                context.getLicense(),
                                response.remoteClusterLicenseInfo().licenseInfo().getType()
                            );
                        }
                    }
                },
                    e -> {
                        context.addValidationError(
                            UNKNOWN_REMOTE_CLUSTER_LICENSE,
                            true,
                            context.getLicense(),
                            remoteAliases,
                            e.getMessage()
                        );
                    }
                ), latch));

            try {
                latch.await();
            } catch (InterruptedException e) {
                context.addValidationError(TIMEOUT_CHECK_REMOTE_CLUSTER_LICENSE, true, context.getLicense(), remoteAliases);
            }
        }
    }

    static class DestinationInSourceValidation implements SourceDestValidation {

        @Override
        public boolean isDeferrable() {
            return true;
        }

        @Override
        public void validate(Context context) {
            final String destIndex = context.getDest();

            for (String src : context.getSource()) {
                if (Regex.simpleMatch(src, destIndex)) {
                    context.addValidationError(DEST_IN_SOURCE, true, destIndex, src);
                    return;
                }
            }

            if (context.resolvedSource.contains(destIndex)) {
                context.addValidationError(DEST_IN_SOURCE, true, destIndex, Strings.arrayToCommaDelimitedString(context.getSource()));
                return;
            }

            if (context.resolvedSource.contains(context.resolveDest())) {
                context.addValidationError(
                    DEST_IN_SOURCE,
                    true,
                    context.resolveDest(),
                    Strings.collectionToCommaDelimitedString(context.resolveSource())
                );
                return;
            }
        }
    }

    static class DestinationSingleIndexValidation implements SourceDestValidation {

        @Override
        public boolean isDeferrable() {
            return false;
        }

        @Override
        public void validate(Context context) {
            context.resolveDest();
        }
    }
}
