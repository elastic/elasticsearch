/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheAction;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheRequest;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheResponse;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor.DOC_TYPE_VALUE;
import static org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor.Fields.APPLICATION;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * {@code NativePrivilegeStore} is a store that reads/writes {@link ApplicationPrivilegeDescriptor} objects,
 * from an Elasticsearch index.
 */
public class NativePrivilegeStore {


    public static final Setting<Integer> CACHE_MAX_APPLICATIONS_SETTING =
        Setting.intSetting("xpack.security.authz.store.privileges.cache.max_size",
            10_000, Setting.Property.NodeScope);

    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting("xpack.security.authz.store.privileges.cache.ttl",
        TimeValue.timeValueHours(24L), Setting.Property.NodeScope);

    private static final Collector<Tuple<String, String>, ?, Map<String, List<String>>> TUPLES_TO_MAP = Collectors.toMap(
        Tuple::v1,
        t -> CollectionUtils.newSingletonArrayList(t.v2()), (a, b) -> {
            a.addAll(b);
            return a;
        });
    private static final Logger logger = LogManager.getLogger(NativePrivilegeStore.class);

    private final Settings settings;
    private final Client client;
    private final SecurityIndexManager securityIndexManager;
    private final Cache<String, Set<ApplicationPrivilegeDescriptor>> descriptorsCache;
    private final Cache<Set<String>, Set<String>> applicationNamesCache;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final ReadWriteLock invalidationLock = new ReentrantReadWriteLock();
    private final ReleasableLock invalidationReadLock = new ReleasableLock(invalidationLock.readLock());
    private final ReleasableLock invalidationWriteLock = new ReleasableLock(invalidationLock.writeLock());


    public NativePrivilegeStore(Settings settings, Client client, SecurityIndexManager securityIndexManager) {
        this.settings = settings;
        this.client = client;
        this.securityIndexManager = securityIndexManager;
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            final int cacheSize = CACHE_MAX_APPLICATIONS_SETTING.get(settings);
            descriptorsCache = CacheBuilder.<String, Set<ApplicationPrivilegeDescriptor>>builder()
                .setMaximumWeight(cacheSize)
                .weigher((k, v) -> v.size())
                .setExpireAfterWrite(ttl).build();
            applicationNamesCache = CacheBuilder.<Set<String>, Set<String>>builder()
                .setMaximumWeight(cacheSize)
                .weigher((k, v) -> k.size() + v.size())
                .setExpireAfterWrite(ttl).build();
        } else {
            descriptorsCache = null;
            applicationNamesCache = null;
        }
        assert (descriptorsCache == null && applicationNamesCache == null)
            || (descriptorsCache != null && applicationNamesCache != null)
            : "descriptor and application names cache must be enabled or disabled together";
    }

    public void getPrivileges(Collection<String> applications, Collection<String> names,
                              ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener) {

        final Set<String> applicationNamesCacheKey = (isEmpty(applications) || applications.contains("*")) ?
            Set.of("*") : Set.copyOf(applications);

        // Always fetch for the concrete application names even when the passed-in application names has no wildcard.
        // This serves as a negative lookup, i.e. when a passed-in non-wildcard application does not exist.
        Set<String> concreteApplicationNames = applicationNamesCache == null ? null : applicationNamesCache.get(applicationNamesCacheKey);

        if (concreteApplicationNames != null && concreteApplicationNames.isEmpty()) {
            logger.debug("returning empty application privileges for [{}] as application names result in empty list",
                applicationNamesCacheKey);
            listener.onResponse(Collections.emptySet());
        } else {
            final Set<ApplicationPrivilegeDescriptor> cachedDescriptors = cachedDescriptorsForApplicationNames(
                concreteApplicationNames != null ? concreteApplicationNames : applicationNamesCacheKey);
            if (cachedDescriptors != null) {
                logger.debug("All application privileges for [{}] found in cache", applicationNamesCacheKey);
                listener.onResponse(filterDescriptorsForPrivilegeNames(cachedDescriptors, names));
            } else {
                final long invalidationCounter = numInvalidation.get();
                // Always fetch all privileges of an application for caching purpose
                logger.debug("Fetching application privilege documents for: {}", applicationNamesCacheKey);
                innerGetPrivileges(applicationNamesCacheKey, ActionListener.wrap(fetchedDescriptors -> {
                    final Map<String, Set<ApplicationPrivilegeDescriptor>> mapOfFetchedDescriptors = fetchedDescriptors.stream()
                        .collect(Collectors.groupingBy(ApplicationPrivilegeDescriptor::getApplication, Collectors.toUnmodifiableSet()));
                    if (descriptorsCache != null) {
                        // Use RWLock and atomic counter to:
                        // 1. Avoid caching potential stale results as much as possible
                        // 2. If stale results are cached, ensure they will be invalidated as soon as possible
                        try (ReleasableLock ignored = invalidationReadLock.acquire()) {
                            if (invalidationCounter == numInvalidation.get()) {
                                cacheFetchedDescriptors(applicationNamesCacheKey, mapOfFetchedDescriptors);
                            }
                        }
                    }
                    listener.onResponse(filterDescriptorsForPrivilegeNames(fetchedDescriptors, names));
                }, listener::onFailure));
            }
        }
    }

    private void innerGetPrivileges(Collection<String> applications, ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener) {
        assert applications != null && applications.size() > 0 : "Application names are required (found " + applications + ")";

        final SecurityIndexManager frozenSecurityIndex = securityIndexManager.freeze();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            securityIndexManager.checkIndexVersionThenExecute(listener::onFailure, () -> {

                final TermQueryBuilder typeQuery = QueryBuilders
                    .termQuery(ApplicationPrivilegeDescriptor.Fields.TYPE.getPreferredName(), DOC_TYPE_VALUE);
                final QueryBuilder query = QueryBuilders.boolQuery().filter(typeQuery)
                    .filter(getApplicationNameQuery(applications));

                final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);
                try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
                    SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                        .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                        .setQuery(query)
                        .setSize(1000)
                        .setFetchSource(true)
                        .request();
                    logger.trace(() ->
                        new ParameterizedMessage("Searching for [{}] privileges with query [{}]",
                            applications, Strings.toString(query)));
                    request.indicesOptions().ignoreUnavailable();
                    ScrollHelper.fetchAllByEntity(client, request, new ContextPreservingActionListener<>(supplier, listener),
                        hit -> buildPrivilege(hit.getId(), hit.getSourceRef()));
                }
            });
        }
    }

    private QueryBuilder getApplicationNameQuery(Collection<String> applications) {
        if (applications.contains("*")) {
            return QueryBuilders.existsQuery(APPLICATION.getPreferredName());
        }
        final List<String> rawNames = new ArrayList<>(applications.size());
        final List<String> wildcardNames = new ArrayList<>(applications.size());
        for (String name : applications) {
            if (name.endsWith("*")) {
                wildcardNames.add(name);
            } else {
                rawNames.add(name);
            }
        }

        assert rawNames.isEmpty() == false || wildcardNames.isEmpty() == false;

        TermsQueryBuilder termsQuery = rawNames.isEmpty() ? null : QueryBuilders.termsQuery(APPLICATION.getPreferredName(), rawNames);
        if (wildcardNames.isEmpty()) {
            return termsQuery;
        }
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (termsQuery != null) {
            boolQuery.should(termsQuery);
        }
        for (String wildcard : wildcardNames) {
            final String prefix = wildcard.substring(0, wildcard.length() - 1);
            boolQuery.should(QueryBuilders.prefixQuery(APPLICATION.getPreferredName(), prefix));
        }
        boolQuery.minimumShouldMatch(1);
        return boolQuery;
    }

    private ApplicationPrivilegeDescriptor buildPrivilege(String docId, BytesReference source) {
        logger.trace("Building privilege from [{}] [{}]", docId, source == null ? "<<null>>" : source.utf8ToString());
        if (source == null) {
            return null;
        }
        final Tuple<String, String> name = nameFromDocId(docId);
        try {
            // EMPTY is safe here because we never use namedObject

            try (StreamInput input = source.streamInput();
                XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, input)) {
                final ApplicationPrivilegeDescriptor privilege = ApplicationPrivilegeDescriptor.parse(parser, null, null, true);
                assert privilege.getApplication().equals(name.v1())
                    : "Incorrect application name for privilege. Expected [" + name.v1() + "] but was " + privilege.getApplication();
                assert privilege.getName().equals(name.v2())
                    : "Incorrect name for application privilege. Expected [" + name.v2() + "] but was " + privilege.getName();
                return privilege;
            }
        } catch (IOException | XContentParseException e) {
            logger.error(new ParameterizedMessage("cannot parse application privilege [{}]", name), e);
            return null;
        }
    }

    /**
     * Try resolve all privileges for given application names from the cache.
     * It returns non-null result only when privileges of ALL applications are
     * found in the cache, i.e. it returns null if any of application name is
     * NOT found in the cache. Since the cached is keyed by concrete application
     * name, this means any wildcard will result in null.
     */
    private Set<ApplicationPrivilegeDescriptor> cachedDescriptorsForApplicationNames(Set<String> applicationNames) {
        if (descriptorsCache == null) {
            return null;
        }
        final Set<ApplicationPrivilegeDescriptor> cachedDescriptors = new HashSet<>();
        for (String applicationName: applicationNames) {
            if (applicationName.endsWith("*")) {
                return null;
            } else {
                final Set<ApplicationPrivilegeDescriptor> descriptors = descriptorsCache.get(applicationName);
                if (descriptors == null) {
                    return null;
                } else {
                    cachedDescriptors.addAll(descriptors);
                }
            }
        }
        return Collections.unmodifiableSet(cachedDescriptors);
    }

    /**
     * Filter to get all privilege descriptors that have any of the given privilege names.
     */
    private Collection<ApplicationPrivilegeDescriptor> filterDescriptorsForPrivilegeNames(
        Collection<ApplicationPrivilegeDescriptor> descriptors, Collection<String> privilegeNames) {
        // empty set of names equals to retrieve everything
        if (isEmpty(privilegeNames)) {
            return descriptors;
        }
        return descriptors.stream().filter(d -> privilegeNames.contains(d.getName())).collect(Collectors.toUnmodifiableSet());
    }

    // protected for tests
    protected void cacheFetchedDescriptors(Set<String> applicationNamesCacheKey,
        Map<String, Set<ApplicationPrivilegeDescriptor>> mapOfFetchedDescriptors) {
        final Set<String> fetchedApplicationNames = Collections.unmodifiableSet(mapOfFetchedDescriptors.keySet());
        // Do not cache the names if expansion has no effect
        if (fetchedApplicationNames.equals(applicationNamesCacheKey) == false) {
            logger.debug("Caching application names query: {} = {}", applicationNamesCacheKey, fetchedApplicationNames);
            applicationNamesCache.put(applicationNamesCacheKey, fetchedApplicationNames);
        }
        for (Map.Entry<String, Set<ApplicationPrivilegeDescriptor>> entry : mapOfFetchedDescriptors.entrySet()) {
            logger.debug("Caching descriptors for application: {}", entry.getKey());
            descriptorsCache.put(entry.getKey(), entry.getValue());
        }
    }

    public void putPrivileges(Collection<ApplicationPrivilegeDescriptor> privileges, WriteRequest.RefreshPolicy refreshPolicy,
                              ActionListener<Map<String, List<String>>> listener) {
        securityIndexManager.prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            ActionListener<IndexResponse> groupListener = new GroupedActionListener<>(
                ActionListener.wrap((Collection<IndexResponse> responses) -> {
                    final Map<String, List<String>> createdNames = responses.stream()
                        .filter(r -> r.getResult() == DocWriteResponse.Result.CREATED)
                        .map(r -> r.getId())
                        .map(NativePrivilegeStore::nameFromDocId)
                        .collect(TUPLES_TO_MAP);
                    clearCaches(listener,
                        privileges.stream().map(ApplicationPrivilegeDescriptor::getApplication).collect(Collectors.toUnmodifiableSet()),
                        createdNames);
                }, listener::onFailure), privileges.size());
            for (ApplicationPrivilegeDescriptor privilege : privileges) {
                innerPutPrivilege(privilege, refreshPolicy, groupListener);
            }
        });
    }

    private void innerPutPrivilege(ApplicationPrivilegeDescriptor privilege, WriteRequest.RefreshPolicy refreshPolicy,
                                   ActionListener<IndexResponse> listener) {
        try {
            final String name = privilege.getName();
            final XContentBuilder xContentBuilder = privilege.toXContent(jsonBuilder(), true);
            ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                client.prepareIndex(SECURITY_MAIN_ALIAS).setId(toDocId(privilege.getApplication(), name))
                    .setSource(xContentBuilder)
                    .setRefreshPolicy(refreshPolicy)
                    .request(), listener, client::index);
        } catch (Exception e) {
            logger.warn("Failed to put privilege {} - {}", Strings.toString(privilege), e.toString());
            listener.onFailure(e);
        }
    }

    public void deletePrivileges(String application, Collection<String> names, WriteRequest.RefreshPolicy refreshPolicy,
                                 ActionListener<Map<String, List<String>>> listener) {
        final SecurityIndexManager frozenSecurityIndex = securityIndexManager.freeze();
        if (frozenSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyMap());
        } else if (frozenSecurityIndex.isAvailable() == false) {
            listener.onFailure(frozenSecurityIndex.getUnavailableReason());
        } else {
            securityIndexManager.checkIndexVersionThenExecute(listener::onFailure, () -> {
                ActionListener<DeleteResponse> groupListener = new GroupedActionListener<>(
                    ActionListener.wrap(responses -> {
                        final Map<String, List<String>> deletedNames = responses.stream()
                            .filter(r -> r.getResult() == DocWriteResponse.Result.DELETED)
                            .map(r -> r.getId())
                            .map(NativePrivilegeStore::nameFromDocId)
                            .collect(TUPLES_TO_MAP);
                        clearCaches(listener, Collections.singleton(application), deletedNames);
                    }, listener::onFailure), names.size());
                for (String name : names) {
                    ClientHelper.executeAsyncWithOrigin(client.threadPool().getThreadContext(), SECURITY_ORIGIN,
                        client.prepareDelete(SECURITY_MAIN_ALIAS, toDocId(application, name))
                            .setRefreshPolicy(refreshPolicy)
                            .request(), groupListener, client::delete);
                }
            });
        }
    }

    private <T> void clearCaches(ActionListener<T> listener, Set<String> applicationNames, T value) {
        // This currently clears _all_ roles, but could be improved to clear only those roles that reference the affected application
        final ClearPrivilegesCacheRequest request = new ClearPrivilegesCacheRequest()
            .applicationNames(applicationNames.toArray(String[]::new)).clearRolesCache(true);
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearPrivilegesCacheAction.INSTANCE, request,
            new ActionListener<>() {
                @Override
                public void onResponse(ClearPrivilegesCacheResponse nodes) {
                    listener.onResponse(value);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("unable to clear application privileges and role cache", e);
                    listener.onFailure(
                        new ElasticsearchException("clearing the application privileges and role cache failed. " +
                            "please clear the caches manually", e));
                }
            });
    }

    private static Tuple<String, String> nameFromDocId(String docId) {
        final String name = docId.substring(DOC_TYPE_VALUE.length() + 1);
        assert name != null && name.length() > 0 : "Invalid name '" + name + "'";
        final int colon = name.indexOf(':');
        assert colon > 0 : "Invalid name '" + name + "' (missing colon)";
        return new Tuple<>(name.substring(0, colon), name.substring(colon + 1));
    }

    private static String toDocId(String application, String name) {
        return DOC_TYPE_VALUE + "_" + application + ":" + name;
    }

    public void onSecurityIndexStateChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState) || isIndexDeleted(previousState, currentState)
            || previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            invalidateAll();
        }
    }

    public void invalidate(Collection<String> updatedApplicationNames) {
        if (descriptorsCache == null) {
            return;
        }
        // Increment the invalidation count to avoid caching stale results
        // Release the lock as soon as we increment the counter so the actual invalidation
        // does not have to block other threads. In theory, there could be very rare edge
        // cases that we would invalidate more than necessary. But we consider less locking
        // duration is overall better.
        try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
            numInvalidation.incrementAndGet();
        }
        logger.debug("Invalidating application privileges caches for: {}", updatedApplicationNames);
        final Set<String> uniqueNames = Set.copyOf(updatedApplicationNames);
        // Always completely invalidate application names cache due to wildcard
        applicationNamesCache.invalidateAll();
        uniqueNames.forEach(descriptorsCache::invalidate);
    }

    public void invalidateAll() {
        if (descriptorsCache == null) {
            return;
        }
        try (ReleasableLock ignored = invalidationWriteLock.acquire()) {
            numInvalidation.incrementAndGet();
        }
        logger.debug("Invalidating all application privileges caches");
        applicationNamesCache.invalidateAll();
        descriptorsCache.invalidateAll();
    }

    private static boolean isEmpty(Collection<String> collection) {
        return collection == null || collection.isEmpty();
    }

    // Package private for tests
    Cache<Set<String>, Set<String>> getApplicationNamesCache() {
        return applicationNamesCache;
    }

    // Package private for tests
    Cache<String, Set<ApplicationPrivilegeDescriptor>> getDescriptorsCache() {
        return descriptorsCache;
    }

    // Package private for tests
    AtomicLong getNumInvalidation() {
        return numInvalidation;
    }
}
