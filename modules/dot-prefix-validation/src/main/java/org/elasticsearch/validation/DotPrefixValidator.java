/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * DotPrefixValidator provides an abstract class implementing a mapped action filter.
 *
 * This class then implements the {@link #apply(Task, String, ActionRequest, ActionListener, ActionFilterChain)}
 * method which checks for indices in the request that begin with a dot, emitting a deprecation
 * warning if they do. If the request is performed by a non-external user (operator, internal product, etc.)
 * as defined by {@link #isInternalRequest()} then the deprecation is emitted. Otherwise, it is skipped.
 *
 * The indices for consideration are returned by the abstract {@link #getIndicesFromRequest(Object)}
 * method, which subclasses must implement.
 *
 * Some built-in index names and patterns are also elided from the check, as defined in
 * {@link #IGNORED_INDEX_NAMES} and {@link #IGNORED_INDEX_PATTERNS}.
 */
public abstract class DotPrefixValidator<RequestType> implements MappedActionFilter {
    public static final Setting<Boolean> VALIDATE_DOT_PREFIXES = Setting.boolSetting(
        "cluster.indices.validate_dot_prefixes",
        true,
        Setting.Property.NodeScope
    );

    /**
     * Names and patterns for indexes where no deprecation should be emitted.
     * Normally we would want to transition these to either system indices, or
     * to use an internal origin for the client. These are shorter-term
     * workarounds until that work can be completed.
     *
     * .elastic-connectors-* is used by enterprise search
     * .ml-* is used by ML
     * .slo-observability-* is used by Observability
     */
    private static Set<String> IGNORED_INDEX_NAMES = Set.of(
        ".elastic-connectors-v1",
        ".elastic-connectors-sync-jobs-v1",
        ".ml-state",
        ".ml-anomalies-unrelated"
    );
    private static Set<Pattern> IGNORED_INDEX_PATTERNS = Set.of(
        Pattern.compile("\\.ml-state-\\d+"),
        Pattern.compile("\\.slo-observability\\.sli-v\\d+.*"),
        Pattern.compile("\\.slo-observability\\.summary-v\\d+.*")
    );

    DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DotPrefixValidator.class);

    private final ThreadContext threadContext;
    private final boolean isEnabled;

    public DotPrefixValidator(ThreadContext threadContext, ClusterService clusterService) {
        this.threadContext = threadContext;
        this.isEnabled = VALIDATE_DOT_PREFIXES.get(clusterService.getSettings());
    }

    protected abstract Set<String> getIndicesFromRequest(RequestType request);

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        Set<String> indices = getIndicesFromRequest((RequestType) request);
        if (isEnabled) {
            validateIndices(indices);
        }
        chain.proceed(task, action, request, listener);
    }

    void validateIndices(@Nullable Set<String> indices) {
        if (indices != null && isInternalRequest() == false) {
            for (String index : indices) {
                if (Strings.hasLength(index)) {
                    char c = getFirstChar(index);
                    if (c == '.') {
                        final String strippedName = stripDateMath(index);
                        if (IGNORED_INDEX_NAMES.contains(strippedName)) {
                            return;
                        }
                        if (IGNORED_INDEX_PATTERNS.stream().anyMatch(p -> p.matcher(strippedName).matches())) {
                            return;
                        }
                        deprecationLogger.warn(
                            DeprecationCategory.INDICES,
                            "dot-prefix",
                            "Index [{}] name begins with a dot (.), which is deprecated, "
                                + "and will not be allowed in a future Elasticsearch version.",
                            index
                        );
                    }
                }
            }
        }
    }

    private static char getFirstChar(String index) {
        char c = index.charAt(0);
        if (c == '<') {
            // Date-math is being used for the index, we need to
            // consider it by stripping the first '<' before we
            // check for a dot-prefix
            String strippedLeading = index.substring(1);
            if (Strings.hasLength(strippedLeading)) {
                c = strippedLeading.charAt(0);
            }
        }
        return c;
    }

    private static String stripDateMath(String index) {
        char c = index.charAt(0);
        if (c == '<') {
            assert index.charAt(index.length() - 1) == '>'
                : "expected index name with date math to start with < and end with >, how did this pass request validation? " + index;
            return index.substring(1, index.length() - 1);
        } else {
            return index;
        }
    }

    boolean isInternalRequest() {
        final String actionOrigin = threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME);
        final boolean isSystemContext = threadContext.isSystemContext();
        final boolean isInternalOrigin = Optional.ofNullable(actionOrigin).map(Strings::hasText).orElse(false);
        final boolean hasElasticOriginHeader = Optional.ofNullable(threadContext.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER))
            .map(Strings::hasText)
            .orElse(false);
        return isSystemContext || isInternalOrigin || hasElasticOriginHeader;
    }
}
