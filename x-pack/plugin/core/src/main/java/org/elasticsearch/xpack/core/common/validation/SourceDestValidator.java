/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.validation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterService;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.util.Map.Entry.comparingByKey;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.validateIndexOrAliasName;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE;

/**
 * Validation of source indexes and destination index.
 *
 * Validations are separated into validators to choose from, e.g. you want to run different types of validations for
 * preview/create/start with or without support for remote clusters
 */
public final class SourceDestValidator {

    // messages
    public static final String SOURCE_INDEX_MISSING = "Source index [{0}] does not exist";
    public static final String DEST_IN_SOURCE = "Destination index [{0}] is included in source expression [{1}]";
    public static final String DEST_LOWERCASE = "Destination index [{0}] must be lowercase";
    public static final String NEEDS_REMOTE_CLUSTER_SEARCH = "Source index is configured with a remote index pattern(s) [{0}]"
        + " but the current node [{1}] is not allowed to connect to remote clusters."
        + " Please enable "
        + REMOTE_CLUSTER_CLIENT_ROLE.roleName()
        + " for all {2} nodes.";
    public static final String ERROR_REMOTE_CLUSTER_SEARCH = "Error resolving remote source: {0}";
    public static final String UNKNOWN_REMOTE_CLUSTER_LICENSE = "Error during license check ({0}) for remote cluster "
        + "alias(es) {1}, error: {2}";
    public static final String FEATURE_NOT_LICENSED_REMOTE_CLUSTER_LICENSE = "License check failed for remote cluster "
        + "alias [{0}], at least a [{1}] license is required, found license [{2}]";
    public static final String REMOTE_CLUSTER_LICENSE_INACTIVE = "License check failed for remote cluster "
        + "alias [{0}], license is not active";
    public static final String REMOTE_SOURCE_INDICES_NOT_SUPPORTED = "remote source indices are not supported";
    public static final String REMOTE_CLUSTERS_TOO_OLD =
        "remote clusters are expected to run at least version [{0}] (reason: [{1}]), but the following clusters were too old: [{2}]";
    public static final String PIPELINE_MISSING = "Pipeline with id [{0}] could not be found";

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final RemoteClusterService remoteClusterService;
    private final RemoteClusterLicenseChecker remoteClusterLicenseChecker;
    private final IngestService ingestService;
    private final String nodeName;
    private final String license;

    /*
     * Internal shared context between validators.
     */
    static class Context {
        private final ClusterState state;
        private final IndexNameExpressionResolver indexNameExpressionResolver;
        private final RemoteClusterService remoteClusterService;
        private final RemoteClusterLicenseChecker remoteClusterLicenseChecker;
        private final IngestService ingestService;
        private final String[] source;
        private final String destIndex;
        private final String destPipeline;
        private final String nodeName;
        private final String license;

        private ValidationException validationException = null;
        private SortedSet<String> resolvedSource = null;
        private SortedSet<String> resolvedRemoteSource = null;
        private String resolvedDest = null;

        Context(
            final ClusterState state,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final RemoteClusterService remoteClusterService,
            final RemoteClusterLicenseChecker remoteClusterLicenseChecker,
            final IngestService ingestService,
            final String[] source,
            final String destIndex,
            final String destPipeline,
            final String nodeName,
            final String license
        ) {
            this.state = state;
            this.indexNameExpressionResolver = indexNameExpressionResolver;
            this.remoteClusterService = remoteClusterService;
            this.remoteClusterLicenseChecker = remoteClusterLicenseChecker;
            this.ingestService = ingestService;
            this.source = source;
            this.destIndex = destIndex;
            this.destPipeline = destPipeline;
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

        public IngestService getIngestService() {
            return ingestService;
        }

        public boolean isRemoteSearchEnabled() {
            return remoteClusterLicenseChecker != null;
        }

        public String[] getSource() {
            return source;
        }

        public String getDestIndex() {
            return destIndex;
        }

        public String getNodeName() {
            return nodeName;
        }

        public String getLicense() {
            return license;
        }

        public SortedSet<String> resolveSource() {
            if (resolvedSource == null) {
                resolveLocalAndRemoteSource();
            }

            return resolvedSource;
        }

        public SortedSet<String> resolveRemoteSource() {
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
                        destIndex,
                        true,
                        false
                    );

                    resolvedDest = singleWriteIndex != null ? singleWriteIndex.getName() : destIndex;
                } catch (IllegalArgumentException e) {
                    // stop here as we can not return a single dest index
                    addValidationError(e.getMessage());
                    throw validationException;
                }
            }

            return resolvedDest;
        }

        public ValidationException addValidationError(String error, Object... args) {
            if (validationException == null) {
                validationException = new ValidationException();
            }

            validationException.addValidationError(getMessage(error, args));

            return validationException;
        }

        public ValidationException getValidationException() {
            return validationException;
        }

        // convenience method to make testing easier
        public Set<String> getRegisteredRemoteClusterNames() {
            return remoteClusterService.getRegisteredRemoteClusterNames();
        }

        // convenience method to make testing easier
        public Version getRemoteClusterVersion(String cluster) {
            return remoteClusterService.getConnection(cluster).getVersion();
        }

        private void resolveLocalAndRemoteSource() {
            resolvedSource = new TreeSet<>(Arrays.asList(source));
            resolvedRemoteSource = new TreeSet<>(RemoteClusterLicenseChecker.remoteIndices(resolvedSource));
            resolvedSource.removeAll(resolvedRemoteSource);

            // special case: if indexNameExpressionResolver gets an empty list it treats it as _all
            if (resolvedSource.isEmpty() == false) {
                resolvedSource = new TreeSet<>(
                    Arrays.asList(
                        indexNameExpressionResolver.concreteIndexNames(
                            state,
                            DEFAULT_INDICES_OPTIONS_FOR_VALIDATION,
                            true,
                            resolvedSource.toArray(Strings.EMPTY_ARRAY)
                        )
                    )
                );
            }
        }
    }

    public interface SourceDestValidation {
        void validate(Context context, ActionListener<Context> listener);
    }

    // note: this is equivalent to the default for search requests
    private static final IndicesOptions DEFAULT_INDICES_OPTIONS_FOR_VALIDATION = IndicesOptions
        .strictExpandOpenAndForbidClosedIgnoreThrottled();

    public static final SourceDestValidation SOURCE_MISSING_VALIDATION = new SourceMissingValidation();
    public static final SourceDestValidation DESTINATION_IN_SOURCE_VALIDATION = new DestinationInSourceValidation();
    public static final SourceDestValidation DESTINATION_SINGLE_INDEX_VALIDATION = new DestinationSingleIndexValidation();
    public static final SourceDestValidation REMOTE_SOURCE_NOT_SUPPORTED_VALIDATION = new RemoteSourceNotSupportedValidation();
    public static final SourceDestValidation DESTINATION_PIPELINE_MISSING_VALIDATION = new DestinationPipelineMissingValidation();

    /**
     * Create a new Source Dest Validator
     *
     * @param indexNameExpressionResolver A valid IndexNameExpressionResolver object
     * @param remoteClusterService A valid RemoteClusterService object
     * @param remoteClusterLicenseChecker A RemoteClusterLicenseChecker or null if CCS is disabled
     * @param nodeName the name of this node
     * @param license the license of the feature validated for
     */
    public SourceDestValidator(
        IndexNameExpressionResolver indexNameExpressionResolver,
        RemoteClusterService remoteClusterService,
        RemoteClusterLicenseChecker remoteClusterLicenseChecker,
        IngestService ingestService,
        String nodeName,
        String license
    ) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.remoteClusterService = remoteClusterService;
        this.remoteClusterLicenseChecker = remoteClusterLicenseChecker;
        this.ingestService = ingestService;
        this.nodeName = nodeName;
        this.license = license;
    }

    /**
     * Run validation against source and destIndex.
     *
     * @param clusterState The current ClusterState
     * @param source an array of source indexes
     * @param destIndex destination index
     * @param destPipeline destination pipeline
     * @param validations list of of validations to run
     * @param listener result listener
     */
    public void validate(
        final ClusterState clusterState,
        final String[] source,
        final String destIndex,
        @Nullable final String destPipeline,
        final List<SourceDestValidation> validations,
        final ActionListener<Boolean> listener
    ) {
        Context context = new Context(
            clusterState,
            indexNameExpressionResolver,
            remoteClusterService,
            remoteClusterLicenseChecker,
            ingestService,
            source,
            destIndex,
            destPipeline,
            nodeName,
            license
        );

        ActionListener<Context> validationListener = ActionListener.wrap(c -> {
            if (c.getValidationException() != null) {
                listener.onFailure(c.getValidationException());
            } else {
                listener.onResponse(true);
            }
        }, listener::onFailure);

        // We traverse the validations in reverse order as we chain the listeners from back to front
        for (int i = validations.size() - 1; i >= 0; i--) {
            SourceDestValidation validation = validations.get(i);
            final ActionListener<Context> previousValidationListener = validationListener;
            validationListener = ActionListener.wrap(c -> validation.validate(c, previousValidationListener), listener::onFailure);
        }

        validationListener.onResponse(context);
    }

    /**
     * Validate request.
     *
     * This runs a couple of simple validations at request time, to be executed from a {@link ActionRequest}}
     * implementation.
     *
     * Note: Source can not be validated at request time as it might contain expressions.
     *
     * @param validationException an ActionRequestValidationException for collection validation problem, can be null
     * @param destIndex destination index, null if validation shall be skipped
     */
    public static ActionRequestValidationException validateRequest(
        @Nullable ActionRequestValidationException validationException,
        @Nullable String destIndex
    ) {
        try {
            if (destIndex != null) {
                validateIndexOrAliasName(destIndex, InvalidIndexNameException::new);
                if (destIndex.toLowerCase(Locale.ROOT).equals(destIndex) == false) {
                    validationException = addValidationError(getMessage(DEST_LOWERCASE, destIndex), validationException);
                }
            }
        } catch (InvalidIndexNameException ex) {
            validationException = addValidationError(ex.getMessage(), validationException);
        }

        return validationException;
    }

    static class SourceMissingValidation implements SourceDestValidation {

        @Override
        public void validate(Context context, ActionListener<Context> listener) {
            try {
                // non-trivia: if source contains a wildcard index, which does not resolve to a concrete index
                // the resolved indices might be empty, but we can check if source contained something, this works because
                // of no wildcard index is involved the resolve would have thrown an exception
                if (context.resolveSource().isEmpty() && context.resolveRemoteSource().isEmpty() && context.getSource().length == 0) {
                    context.addValidationError(SOURCE_INDEX_MISSING, Strings.arrayToCommaDelimitedString(context.getSource()));
                }
            } catch (IndexNotFoundException e) {
                context.addValidationError(e.getMessage());
            }
            listener.onResponse(context);
        }
    }

    public static class RemoteSourceEnabledAndRemoteLicenseValidation implements SourceDestValidation {

        private final String nodeRoleThatRequiresRemoteClusterClient;

        public RemoteSourceEnabledAndRemoteLicenseValidation(String nodeRoleThatRequiresRemoteClusterClient) {
            this.nodeRoleThatRequiresRemoteClusterClient = nodeRoleThatRequiresRemoteClusterClient;
        }

        @Override
        public void validate(Context context, ActionListener<Context> listener) {
            if (context.resolveRemoteSource().isEmpty()) {
                listener.onResponse(context);
                return;
            }

            List<String> remoteIndices = new ArrayList<>(context.resolveRemoteSource());
            // we can only check this node at the moment, clusters with mixed CCS enabled/disabled nodes are not supported,
            // see gh#50033
            if (context.isRemoteSearchEnabled() == false) {
                context.addValidationError(
                    NEEDS_REMOTE_CLUSTER_SEARCH,
                    context.resolveRemoteSource(),
                    context.getNodeName(),
                    nodeRoleThatRequiresRemoteClusterClient
                );
                listener.onResponse(context);
                return;
            }

            // this can throw
            List<String> remoteAliases;
            try {
                remoteAliases = RemoteClusterLicenseChecker.remoteClusterAliases(context.getRegisteredRemoteClusterNames(), remoteIndices);
            } catch (NoSuchRemoteClusterException e) {
                context.addValidationError(e.getMessage());
                listener.onResponse(context);
                return;
            } catch (Exception e) {
                context.addValidationError(ERROR_REMOTE_CLUSTER_SEARCH, e.getMessage());
                listener.onResponse(context);
                return;
            }

            context.getRemoteClusterLicenseChecker().checkRemoteClusterLicenses(remoteAliases, ActionListener.wrap(response -> {
                if (response.isSuccess() == false) {
                    if (response.remoteClusterLicenseInfo().licenseInfo().getStatus() != LicenseStatus.ACTIVE) {
                        context.addValidationError(REMOTE_CLUSTER_LICENSE_INACTIVE, response.remoteClusterLicenseInfo().clusterAlias());
                    } else {
                        context.addValidationError(
                            FEATURE_NOT_LICENSED_REMOTE_CLUSTER_LICENSE,
                            response.remoteClusterLicenseInfo().clusterAlias(),
                            context.getLicense(),
                            response.remoteClusterLicenseInfo().licenseInfo().getType()
                        );
                    }
                }
                listener.onResponse(context);
            }, e -> {
                context.addValidationError(UNKNOWN_REMOTE_CLUSTER_LICENSE, context.getLicense(), remoteAliases, e.getMessage());
                listener.onResponse(context);
            }));
        }
    }

    public static class RemoteClusterMinimumVersionValidation implements SourceDestValidation {

        private final Version minExpectedVersion;
        private final String reason;

        public RemoteClusterMinimumVersionValidation(Version minExpectedVersion, String reason) {
            this.minExpectedVersion = minExpectedVersion;
            this.reason = reason;
        }

        public Version getMinExpectedVersion() {
            return minExpectedVersion;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public void validate(Context context, ActionListener<Context> listener) {
            List<String> remoteIndices = new ArrayList<>(context.resolveRemoteSource());
            Map<String, Version> remoteClusterVersions;
            try {
                List<String> remoteAliases = RemoteClusterLicenseChecker.remoteClusterAliases(
                    context.getRegisteredRemoteClusterNames(),
                    remoteIndices
                );
                remoteClusterVersions = remoteAliases.stream().collect(toMap(identity(), context::getRemoteClusterVersion));
            } catch (NoSuchRemoteClusterException e) {
                context.addValidationError(e.getMessage());
                listener.onResponse(context);
                return;
            } catch (Exception e) {
                context.addValidationError(ERROR_REMOTE_CLUSTER_SEARCH, e.getMessage());
                listener.onResponse(context);
                return;
            }
            Map<String, Version> oldRemoteClusterVersions = remoteClusterVersions.entrySet()
                .stream()
                .filter(entry -> entry.getValue().before(minExpectedVersion))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (oldRemoteClusterVersions.isEmpty() == false) {
                context.addValidationError(
                    REMOTE_CLUSTERS_TOO_OLD,
                    minExpectedVersion,
                    reason,
                    oldRemoteClusterVersions.entrySet()
                        .stream()
                        .sorted(comparingByKey())  // sort to have a deterministic order among clusters in the resulting string
                        .map(e -> e.getKey() + " (" + e.getValue() + ")")
                        .collect(joining(", "))
                );
            }
            listener.onResponse(context);
        }
    }

    static class DestinationInSourceValidation implements SourceDestValidation {

        @Override
        public void validate(Context context, ActionListener<Context> listener) {
            final String destIndex = context.getDestIndex();
            boolean foundSourceInDest = false;

            for (String src : context.getSource()) {
                if (Regex.simpleMatch(src, destIndex)) {
                    context.addValidationError(DEST_IN_SOURCE, destIndex, src);
                    // do not return immediately but collect all errors and than return
                    foundSourceInDest = true;
                }
            }

            if (foundSourceInDest) {
                listener.onResponse(context);
                return;
            }

            if (context.resolveSource().contains(destIndex)) {
                context.addValidationError(DEST_IN_SOURCE, destIndex, Strings.arrayToCommaDelimitedString(context.getSource()));
                listener.onResponse(context);
                return;
            }

            if (context.resolveSource().contains(context.resolveDest())) {
                context.addValidationError(
                    DEST_IN_SOURCE,
                    context.resolveDest(),
                    Strings.collectionToCommaDelimitedString(context.resolveSource())
                );
            }

            listener.onResponse(context);
        }
    }

    static class DestinationSingleIndexValidation implements SourceDestValidation {

        @Override
        public void validate(Context context, ActionListener<Context> listener) {
            context.resolveDest();
            listener.onResponse(context);
        }
    }

    static class RemoteSourceNotSupportedValidation implements SourceDestValidation {

        @Override
        public void validate(Context context, ActionListener<Context> listener) {
            if (context.resolveRemoteSource().isEmpty() == false) {
                context.addValidationError(REMOTE_SOURCE_INDICES_NOT_SUPPORTED);
            }
            listener.onResponse(context);
        }
    }

    static class DestinationPipelineMissingValidation implements SourceDestValidation {

        @Override
        public void validate(Context context, ActionListener<Context> listener) {
            if (context.destPipeline != null) {
                if (context.ingestService.getPipeline(context.destPipeline) == null) {
                    context.addValidationError(PIPELINE_MISSING, context.destPipeline);
                }
            }
            listener.onResponse(context);
        }
    }

    private static String getMessage(String message, Object... args) {
        return new MessageFormat(message, Locale.ROOT).format(args);
    }
}
