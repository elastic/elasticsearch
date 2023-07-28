/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReferenceResolver;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.authz.store.RolesRetrievalResult;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;

public class RoleDescriptorStore implements RoleReferenceResolver {

    private static final Logger logger = LogManager.getLogger(RoleDescriptorStore.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RoleDescriptorStore.class);

    private final RoleProviders roleProviders;
    private final ApiKeyService apiKeyService;
    private final ServiceAccountService serviceAccountService;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;
    private final Consumer<Collection<RoleDescriptor>> effectiveRoleDescriptorsConsumer;
    private final Cache<String, Boolean> negativeLookupCache;

    public RoleDescriptorStore(
        RoleProviders roleProviders,
        ApiKeyService apiKeyService,
        ServiceAccountService serviceAccountService,
        Cache<String, Boolean> negativeLookupCache,
        XPackLicenseState licenseState,
        ThreadContext threadContext,
        Consumer<Collection<RoleDescriptor>> effectiveRoleDescriptorsConsumer
    ) {
        this.roleProviders = roleProviders;
        this.apiKeyService = Objects.requireNonNull(apiKeyService);
        this.serviceAccountService = Objects.requireNonNull(serviceAccountService);
        this.licenseState = Objects.requireNonNull(licenseState);
        this.threadContext = threadContext;
        this.effectiveRoleDescriptorsConsumer = Objects.requireNonNull(effectiveRoleDescriptorsConsumer);
        this.negativeLookupCache = negativeLookupCache;
    }

    @Override
    public void resolveNamedRoleReference(
        RoleReference.NamedRoleReference namedRoleReference,
        ActionListener<RolesRetrievalResult> listener
    ) {
        final Set<String> roleNames = Set.copyOf(new HashSet<>(List.of(namedRoleReference.getRoleNames())));
        if (roleNames.isEmpty()) {
            listener.onResponse(RolesRetrievalResult.EMPTY);
        } else if (roleNames.equals(Set.of(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName()))) {
            listener.onResponse(RolesRetrievalResult.SUPERUSER);
        } else {
            resolveRoleNames(roleNames, listener);
        }
    }

    @Override
    public void resolveApiKeyRoleReference(
        RoleReference.ApiKeyRoleReference apiKeyRoleReference,
        ActionListener<RolesRetrievalResult> listener
    ) {
        final List<RoleDescriptor> roleDescriptors = apiKeyService.parseRoleDescriptorsBytes(
            apiKeyRoleReference.getApiKeyId(),
            apiKeyRoleReference.getRoleDescriptorsBytes(),
            apiKeyRoleReference.getRoleType()
        );
        final RolesRetrievalResult rolesRetrievalResult = new RolesRetrievalResult();
        rolesRetrievalResult.addDescriptors(Set.copyOf(roleDescriptors));
        assert (apiKeyRoleReference.getRoleType() == RoleReference.ApiKeyRoleType.ASSIGNED
            && rolesRetrievalResult.getRoleDescriptors().stream().filter(RoleDescriptor::hasRestriction).count() <= 1)
            || (apiKeyRoleReference.getRoleType() == RoleReference.ApiKeyRoleType.LIMITED_BY
                && rolesRetrievalResult.getRoleDescriptors().stream().noneMatch(RoleDescriptor::hasRestriction))
            : "there should be zero limited-by role descriptors with restriction and no more than one assigned";
        listener.onResponse(rolesRetrievalResult);
    }

    @Override
    public void resolveBwcApiKeyRoleReference(
        RoleReference.BwcApiKeyRoleReference bwcApiKeyRoleReference,
        ActionListener<RolesRetrievalResult> listener
    ) {
        final List<RoleDescriptor> roleDescriptors = apiKeyService.parseRoleDescriptors(
            bwcApiKeyRoleReference.getApiKeyId(),
            bwcApiKeyRoleReference.getRoleDescriptorsMap(),
            bwcApiKeyRoleReference.getRoleType()
        );
        final RolesRetrievalResult rolesRetrievalResult = new RolesRetrievalResult();
        rolesRetrievalResult.addDescriptors(Set.copyOf(roleDescriptors));
        listener.onResponse(rolesRetrievalResult);
    }

    @Override
    public void resolveServiceAccountRoleReference(
        RoleReference.ServiceAccountRoleReference roleReference,
        ActionListener<RolesRetrievalResult> listener
    ) {
        serviceAccountService.getRoleDescriptorForPrincipal(roleReference.getPrincipal(), listener.map(roleDescriptor -> {
            final RolesRetrievalResult rolesRetrievalResult = new RolesRetrievalResult();
            rolesRetrievalResult.addDescriptors(Set.of(roleDescriptor));
            return rolesRetrievalResult;
        }));
    }

    @Override
    public void resolveCrossClusterAccessRoleReference(
        RoleReference.CrossClusterAccessRoleReference crossClusterAccessRoleReference,
        ActionListener<RolesRetrievalResult> listener
    ) {
        final Set<RoleDescriptor> roleDescriptors = crossClusterAccessRoleReference.getRoleDescriptorsBytes().toRoleDescriptors();
        for (RoleDescriptor roleDescriptor : roleDescriptors) {
            if (roleDescriptor.hasPrivilegesOtherThanIndex()) {
                final String message = "Role descriptor for cross cluster access can only contain index privileges "
                    + "but other privileges found for subject ["
                    + crossClusterAccessRoleReference.getUserPrincipal()
                    + "]";
                logger.debug("{}. Invalid role descriptor: [{}]", message, roleDescriptor);
                listener.onFailure(new IllegalArgumentException(message));
                return;
            }
        }
        if (roleDescriptors.isEmpty()) {
            logger.debug(
                () -> "Cross cluster access role reference ["
                    + crossClusterAccessRoleReference.id()
                    + "] resolved to an empty role descriptor set"
            );
            listener.onResponse(RolesRetrievalResult.EMPTY);
            return;
        }
        final RolesRetrievalResult rolesRetrievalResult = new RolesRetrievalResult();
        rolesRetrievalResult.addDescriptors(Set.copyOf(roleDescriptors));
        listener.onResponse(rolesRetrievalResult);
    }

    private void resolveRoleNames(Set<String> roleNames, ActionListener<RolesRetrievalResult> listener) {
        roleDescriptors(roleNames, ActionListener.wrap(rolesRetrievalResult -> {
            logDeprecatedRoles(rolesRetrievalResult.getRoleDescriptors());
            final boolean missingRoles = rolesRetrievalResult.getMissingRoles().isEmpty() == false;
            if (missingRoles) {
                logger.debug(() -> format("Could not find roles with names %s", rolesRetrievalResult.getMissingRoles()));
            }
            final Set<RoleDescriptor> effectiveDescriptors = maybeSkipRolesUsingDocumentOrFieldLevelSecurity(
                rolesRetrievalResult.getRoleDescriptors()
            );
            logger.trace(() -> format("Exposing effective role descriptors [%s] for role names [%s]", effectiveDescriptors, roleNames));
            effectiveRoleDescriptorsConsumer.accept(Collections.unmodifiableCollection(effectiveDescriptors));
            // TODO: why not populate negativeLookupCache here with missing roles?

            // TODO: replace with a class that better represent the result, e.g. carry info for disabled role
            final RolesRetrievalResult finalResult = new RolesRetrievalResult();
            finalResult.addDescriptors(effectiveDescriptors);
            finalResult.setMissingRoles(rolesRetrievalResult.getMissingRoles());
            if (false == rolesRetrievalResult.isSuccess()) {
                finalResult.setFailure();
            }
            listener.onResponse(finalResult);
        }, listener::onFailure));
    }

    private Set<RoleDescriptor> maybeSkipRolesUsingDocumentOrFieldLevelSecurity(Set<RoleDescriptor> roleDescriptors) {
        if (shouldSkipRolesUsingDocumentOrFieldLevelSecurity(roleDescriptors) == false) {
            return roleDescriptors;
        }

        final Map<Boolean, Set<RoleDescriptor>> partitionedRoleDescriptors = roleDescriptors.stream()
            .collect(Collectors.partitioningBy(RoleDescriptor::isUsingDocumentOrFieldLevelSecurity, Collectors.toSet()));

        final Set<RoleDescriptor> roleDescriptorsToSkip = partitionedRoleDescriptors.get(true);
        logger.warn(
            "User roles [{}] are disabled because they require field or document level security. "
                + "The current license is non-compliant for [field and document level security].",
            roleDescriptorsToSkip.stream().map(RoleDescriptor::getName).collect(Collectors.joining(","))
        );

        return partitionedRoleDescriptors.get(false);
    }

    private boolean shouldSkipRolesUsingDocumentOrFieldLevelSecurity(Set<RoleDescriptor> roleDescriptors) {
        return roleDescriptors.stream().anyMatch(RoleDescriptor::isUsingDocumentOrFieldLevelSecurity)
            && DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState) == false;
    }

    private void roleDescriptors(Set<String> roleNames, ActionListener<RolesRetrievalResult> rolesResultListener) {
        final Set<String> filteredRoleNames = roleNames.stream().filter((s) -> {
            if (negativeLookupCache.get(s) != null) {
                logger.debug(() -> "Requested role [" + s + "] does not exist (cached)");
                return false;
            } else {
                return true;
            }
        }).collect(Collectors.toSet());

        loadRoleDescriptorsAsync(filteredRoleNames, rolesResultListener);
    }

    void logDeprecatedRoles(Set<RoleDescriptor> roleDescriptors) {
        roleDescriptors.stream()
            .filter(rd -> Boolean.TRUE.equals(rd.getMetadata().get(MetadataUtils.DEPRECATED_METADATA_KEY)))
            .forEach(rd -> {
                String reason = Objects.toString(
                    rd.getMetadata().get(MetadataUtils.DEPRECATED_REASON_METADATA_KEY),
                    "Please check the documentation"
                );
                deprecationLogger.critical(
                    DeprecationCategory.SECURITY,
                    "deprecated_role-" + rd.getName(),
                    "The role [" + rd.getName() + "] is deprecated and will be removed in a future version of Elasticsearch. " + reason
                );
            });
    }

    private void loadRoleDescriptorsAsync(Set<String> roleNames, ActionListener<RolesRetrievalResult> listener) {
        final RolesRetrievalResult rolesResult = new RolesRetrievalResult();
        final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> asyncRoleProviders = roleProviders.getProviders();
        final ActionListener<RoleRetrievalResult> descriptorsListener = ContextPreservingActionListener.wrapPreservingContext(
            ActionListener.wrap(ignore -> {
                rolesResult.setMissingRoles(roleNames);
                listener.onResponse(rolesResult);
            }, listener::onFailure),
            threadContext
        );

        final Predicate<RoleRetrievalResult> iterationPredicate = result -> roleNames.isEmpty() == false;
        new IteratingActionListener<>(descriptorsListener, (rolesProvider, providerListener) -> {
            // try to resolve descriptors with role provider
            rolesProvider.accept(roleNames, ActionListener.wrap(result -> {
                if (result.isSuccess()) {
                    logger.debug(
                        () -> format(
                            "Roles [%s] were resolved by [%s]",
                            result.getDescriptors().stream().map(RoleDescriptor::getName).collect(joining(",")),
                            rolesProvider
                        )
                    );
                    final Set<RoleDescriptor> resolvedDescriptors = result.getDescriptors();
                    rolesResult.addDescriptors(resolvedDescriptors);
                    // remove resolved descriptors from the set of roles still needed to be resolved
                    for (RoleDescriptor descriptor : resolvedDescriptors) {
                        roleNames.remove(descriptor.getName());
                    }
                } else {
                    logger.warn(() -> format("role [%s] retrieval failed from [%s]", roleNames, rolesProvider), result.getFailure());
                    rolesResult.setFailure();
                }
                providerListener.onResponse(result);
            }, providerListener::onFailure));
        }, asyncRoleProviders, threadContext, Function.identity(), iterationPredicate).run();
    }

}
