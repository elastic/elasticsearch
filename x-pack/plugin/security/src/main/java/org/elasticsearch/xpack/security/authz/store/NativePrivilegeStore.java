/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.ScrollHelper;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheAction;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheRequest;
import org.elasticsearch.xpack.core.security.action.privilege.ClearPrivilegesCacheResponse;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.support.StringMatcher;
import org.elasticsearch.xpack.security.support.CacheInvalidatorRegistry;
import org.elasticsearch.xpack.security.support.LockingAtomicCounter;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.search.SearchService.DEFAULT_KEEPALIVE_SETTING;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor.DOC_TYPE_VALUE;
import static org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor.Fields.APPLICATION;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;

/**
 * {@code NativePrivilegeStore} is a store that reads/writes {@link ApplicationPrivilegeDescriptor} objects,
 * from an Elasticsearch index.
 */
public class NativePrivilegeStore {

    public static final Setting<Integer> CACHE_MAX_APPLICATIONS_SETTING = Setting.intSetting(
        "xpack.security.authz.store.privileges.cache.max_size",
        10_000,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting(
        "xpack.security.authz.store.privileges.cache.ttl",
        TimeValue.timeValueHours(24L),
        Setting.Property.NodeScope
    );

    private static final Collector<Tuple<String, String>, ?, Map<String, List<String>>> TUPLES_TO_MAP = Collectors.toMap(
        Tuple::v1,
        t -> CollectionUtils.newSingletonArrayList(t.v2()),
        (a, b) -> {
            a.addAll(b);
            return a;
        }
    );
    private static final Logger logger = LogManager.getLogger(NativePrivilegeStore.class);

    private final Settings settings;
    private final Client client;
    private final SecurityIndexManager securityIndexManager;
    private volatile boolean allowExpensiveQueries;
    private final DescriptorsAndApplicationNamesCache descriptorsAndApplicationNamesCache;

    public NativePrivilegeStore(
        Settings settings,
        Client client,
        SecurityIndexManager securityIndexManager,
        CacheInvalidatorRegistry cacheInvalidatorRegistry,
        ClusterService clusterService
    ) {
        this.settings = settings;
        this.client = client;
        this.securityIndexManager = securityIndexManager;
        this.allowExpensiveQueries = ALLOW_EXPENSIVE_QUERIES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ALLOW_EXPENSIVE_QUERIES, this::setAllowExpensiveQueries);
        final TimeValue ttl = CACHE_TTL_SETTING.get(settings);
        if (ttl.getNanos() > 0) {
            final ThreadPool threadPool = client.threadPool();
            descriptorsAndApplicationNamesCache = new DescriptorsAndApplicationNamesCache(
                ttl,
                CACHE_MAX_APPLICATIONS_SETTING.get(settings),
                threadPool.generic(),
                threadPool.getThreadContext()
            );
            cacheInvalidatorRegistry.registerCacheInvalidator("application_privileges", descriptorsAndApplicationNamesCache);
        } else {
            descriptorsAndApplicationNamesCache = null;
        }
    }

    public void getPrivileges(
        Collection<String> applications,
        Collection<String> names,
        ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
    ) {
        getPrivileges(applications, names, true, listener);
    }

    public void getPrivileges(
        Collection<String> applications,
        Collection<String> names,
        boolean waitForAvailableSecurityIndex,
        ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
    ) {
        if (false == isEmpty(names) && names.stream().noneMatch(ApplicationPrivilege::isValidPrivilegeName)) {
            logger.debug("no concrete privilege, only action patterns [{}], returning no application privilege descriptors", names);
            listener.onResponse(Collections.emptySet());
            return;
        }

        final Set<String> applicationNamesCacheKey = (isEmpty(applications) || applications.contains("*"))
            ? Set.of("*")
            : Set.copyOf(applications);

        final ActionListener<Collection<ApplicationPrivilegeDescriptor>> filteringListener = listener.map(
            descriptors -> filterDescriptorsForPrivilegeNames(descriptors, names)
        );
        if (descriptorsAndApplicationNamesCache == null) {
            innerGetPrivileges(applicationNamesCacheKey, waitForAvailableSecurityIndex, filteringListener);
        } else {
            descriptorsAndApplicationNamesCache.get(
                applicationNamesCacheKey,
                waitForAvailableSecurityIndex,
                loadListener -> innerGetPrivileges(applicationNamesCacheKey, waitForAvailableSecurityIndex, loadListener),
                filteringListener
            );
        }
    }

    private void innerGetPrivileges(
        Collection<String> applications,
        boolean waitForAvailableSecurityIndex,
        ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
    ) {
        assert applications != null && applications.size() > 0 : "Application names are required (found " + applications + ")";

        final IndexState projectSecurityIndex = securityIndexManager.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyList());
        } else if (projectSecurityIndex.isAvailable(SEARCH_SHARDS) == false) {
            if (false == waitForAvailableSecurityIndex) {
                listener.onFailure(projectSecurityIndex.getUnavailableReason(SEARCH_SHARDS));
                return;
            }
            securityIndexManager.tryAwaitIndexAvailableForSearch(
                ActionListener.wrap(
                    latest -> latest.checkIndexVersionThenExecute(listener::onFailure, () -> searchPrivileges(applications, listener)),
                    e -> {
                        if (e instanceof IndexNotFoundException) {
                            listener.onResponse(Collections.emptyList());
                        } else {
                            listener.onFailure(e);
                        }
                    }
                )
            );
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> searchPrivileges(applications, listener));
        }
    }

    private void searchPrivileges(Collection<String> applications, ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener) {
        final TermQueryBuilder typeQuery = QueryBuilders.termQuery(
            ApplicationPrivilegeDescriptor.Fields.TYPE.getPreferredName(),
            DOC_TYPE_VALUE
        );
        final Tuple<QueryBuilder, Predicate<String>> applicationNameQueryAndPredicate = getApplicationNameQueryAndPredicate(applications);

        final QueryBuilder query;
        if (applicationNameQueryAndPredicate.v1() != null) {
            query = QueryBuilders.boolQuery().filter(typeQuery).filter(applicationNameQueryAndPredicate.v1());
        } else {
            query = QueryBuilders.boolQuery().filter(typeQuery);
        }

        final Supplier<ThreadContext.StoredContext> supplier = client.threadPool().getThreadContext().newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            SearchRequest request = client.prepareSearch(SECURITY_MAIN_ALIAS)
                .setScroll(DEFAULT_KEEPALIVE_SETTING.get(settings))
                .setQuery(query)
                .setSize(1000)
                .setFetchSource(true)
                .request();
            logger.trace(() -> format("Searching for [%s] privileges with query [%s]", applications, Strings.toString(query)));
            ScrollHelper.fetchAllByEntity(
                client,
                request,
                new ContextPreservingActionListener<>(supplier, listener),
                hit -> buildPrivilege(hit.getId(), hit.getSourceRef(), applicationNameQueryAndPredicate.v2())
            );
        }
    }

    public SecurityIndexManager getSecurityIndexManager() {
        return securityIndexManager;
    }

    private Tuple<QueryBuilder, Predicate<String>> getApplicationNameQueryAndPredicate(Collection<String> applications) {
        if (applications.contains("*")) {
            return new Tuple<>(QueryBuilders.existsQuery(APPLICATION.getPreferredName()), null);
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
            return new Tuple<>(termsQuery, null);
        }

        if (allowExpensiveQueries) {
            final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            if (termsQuery != null) {
                boolQuery.should(termsQuery);
            }
            for (String wildcard : wildcardNames) {
                final String prefix = wildcard.substring(0, wildcard.length() - 1);
                boolQuery.should(QueryBuilders.prefixQuery(APPLICATION.getPreferredName(), prefix));
            }
            boolQuery.minimumShouldMatch(1);
            return new Tuple<>(boolQuery, null);
        } else {
            logger.trace("expensive queries are not allowed, switching to filtering application names in memory");
            return new Tuple<>(null, StringMatcher.of(applications));
        }
    }

    private void setAllowExpensiveQueries(boolean allowExpensiveQueries) {
        this.allowExpensiveQueries = allowExpensiveQueries;
    }

    private static ApplicationPrivilegeDescriptor buildPrivilege(
        String docId,
        BytesReference source,
        @Nullable Predicate<String> applicationNamePredicate
    ) {
        logger.trace("Building privilege from [{}] [{}]", docId, source == null ? "<<null>>" : source.utf8ToString());
        if (source == null) {
            return null;
        }
        final Tuple<String, String> name = nameFromDocId(docId);
        if (applicationNamePredicate != null && false == applicationNamePredicate.test(name.v1())) {
            return null;
        }
        try {
            // EMPTY is safe here because we never use namedObject

            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                    source,
                    XContentType.JSON
                )
            ) {
                final ApplicationPrivilegeDescriptor privilege = ApplicationPrivilegeDescriptor.parse(parser, null, null, true);
                assert privilege.getApplication().equals(name.v1())
                    : "Incorrect application name for privilege. Expected [" + name.v1() + "] but was " + privilege.getApplication();
                assert privilege.getName().equals(name.v2())
                    : "Incorrect name for application privilege. Expected [" + name.v2() + "] but was " + privilege.getName();
                return privilege;
            }
        } catch (IOException | XContentParseException e) {
            logger.error(() -> "cannot parse application privilege [" + name + "]", e);
            return null;
        }
    }

    /**
     * Filter to get all privilege descriptors that have any of the given privilege names.
     */
    private static Collection<ApplicationPrivilegeDescriptor> filterDescriptorsForPrivilegeNames(
        Collection<ApplicationPrivilegeDescriptor> descriptors,
        Collection<String> privilegeNames
    ) {
        // empty set of names equals to retrieve everything
        if (isEmpty(privilegeNames)) {
            return descriptors;
        }
        return descriptors.stream().filter(d -> privilegeNames.contains(d.getName())).collect(Collectors.toUnmodifiableSet());
    }

    public void putPrivileges(
        Collection<ApplicationPrivilegeDescriptor> privileges,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<Map<String, Map<String, DocWriteResponse.Result>>> listener
    ) {
        if (privileges.isEmpty()) {
            listener.onResponse(Map.of());
            return;
        }

        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(refreshPolicy);

        try {
            for (ApplicationPrivilegeDescriptor privilege : privileges) {
                bulkRequestBuilder.add(preparePutPrivilege(privilege));
            }
        } catch (IOException e) {
            listener.onFailure(e);
        }

        securityIndexManager.forCurrentProject().prepareIndexIfNeededThenExecute(listener::onFailure, () -> {
            ClientHelper.executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                bulkRequestBuilder.request(),
                ActionListener.<BulkResponse>wrap(bulkResponse -> handleBulkResponse(bulkResponse, listener), ex -> {
                    logger.warn(Strings.format("Failed to write application privileges to %s", securityIndexManager.aliasName()), ex);
                    listener.onFailure(ex);
                }),
                client::bulk
            );
        });
    }

    private IndexRequest preparePutPrivilege(ApplicationPrivilegeDescriptor privilege) throws IOException {
        try {
            final String name = privilege.getName();
            final XContentBuilder xContentBuilder = privilege.toXContent(jsonBuilder(), true);
            return client.prepareIndex(SECURITY_MAIN_ALIAS)
                .setId(toDocId(privilege.getApplication(), name))
                .setSource(xContentBuilder)
                .request();
        } catch (IOException e) {
            logger.warn("Failed to build application privilege {} - {}", Strings.toString(privilege), e.toString());
            throw e;
        }
    }

    private void handleBulkResponse(BulkResponse bulkResponse, ActionListener<Map<String, Map<String, DocWriteResponse.Result>>> listener) {
        ElasticsearchException failure = null;
        final Map<String, Map<String, DocWriteResponse.Result>> privilegeResultByAppName = new HashMap<>();
        for (var item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                if (failure == null) {
                    failure = new ElasticsearchException("Failed to put application privileges", item.getFailure().getCause());
                } else {
                    failure.addSuppressed(item.getFailure().getCause());
                }
            } else {
                final Tuple<String, String> name = nameFromDocId(item.getId());
                final String appName = name.v1();
                final String privilegeName = name.v2();

                var privileges = privilegeResultByAppName.get(appName);
                if (privileges == null) {
                    privileges = new HashMap<>();
                    privilegeResultByAppName.put(appName, privileges);
                }
                privileges.put(privilegeName, item.getResponse().getResult());
            }
        }
        if (failure != null) {
            listener.onFailure(failure);
        } else {
            clearCaches(listener, privilegeResultByAppName.keySet(), privilegeResultByAppName);
        }
    }

    public void deletePrivileges(
        String application,
        Collection<String> names,
        WriteRequest.RefreshPolicy refreshPolicy,
        ActionListener<Map<String, List<String>>> listener
    ) {
        final IndexState projectSecurityIndex = securityIndexManager.forCurrentProject();
        if (projectSecurityIndex.indexExists() == false) {
            listener.onResponse(Collections.emptyMap());
        } else if (projectSecurityIndex.isAvailable(PRIMARY_SHARDS) == false) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(PRIMARY_SHARDS));
        } else {
            projectSecurityIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                ActionListener<DeleteResponse> groupListener = new GroupedActionListener<>(names.size(), ActionListener.wrap(responses -> {
                    final Map<String, List<String>> deletedNames = responses.stream()
                        .filter(r -> r.getResult() == DocWriteResponse.Result.DELETED)
                        .map(r -> r.getId())
                        .map(NativePrivilegeStore::nameFromDocId)
                        .collect(TUPLES_TO_MAP);
                    clearCaches(listener, Collections.singleton(application), deletedNames);
                }, listener::onFailure));
                for (String name : names) {
                    ClientHelper.executeAsyncWithOrigin(
                        client.threadPool().getThreadContext(),
                        SECURITY_ORIGIN,
                        client.prepareDelete(SECURITY_MAIN_ALIAS, toDocId(application, name)).setRefreshPolicy(refreshPolicy).request(),
                        groupListener,
                        client::delete
                    );
                }
            });
        }
    }

    private <T> void clearCaches(ActionListener<T> listener, Set<String> applicationNames, T value) {
        // This currently clears _all_ roles, but could be improved to clear only those roles that reference the affected application
        final ClearPrivilegesCacheRequest request = new ClearPrivilegesCacheRequest().applicationNames(
            applicationNames.toArray(String[]::new)
        ).clearRolesCache(true);
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, ClearPrivilegesCacheAction.INSTANCE, request, new ActionListener<>() {
            @Override
            public void onResponse(ClearPrivilegesCacheResponse nodes) {
                listener.onResponse(value);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("unable to clear application privileges and role cache", e);
                listener.onFailure(
                    new ElasticsearchException(
                        "clearing the application privileges and role cache failed, please clear the caches manually",
                        e
                    )
                );
            }
        });
    }

    /**
     * @return A Tuple of (application-name, privilege-name)
     */
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

    private static boolean isEmpty(Collection<String> collection) {
        return collection == null || collection.isEmpty();
    }

    // Package private for tests
    DescriptorsAndApplicationNamesCache getDescriptorsAndApplicationNamesCache() {
        return descriptorsAndApplicationNamesCache;
    }

    // Package private for tests
    Cache<Set<String>, Set<String>> getApplicationNamesCache() {
        return descriptorsAndApplicationNamesCache == null ? null : descriptorsAndApplicationNamesCache.applicationNamesCache;
    }

    // Package private for tests
    Cache<String, Set<ApplicationPrivilegeDescriptor>> getDescriptorsCache() {
        return descriptorsAndApplicationNamesCache == null ? null : descriptorsAndApplicationNamesCache.descriptorsCache;
    }

    // Package private for tests
    long getNumInvalidation() {
        return descriptorsAndApplicationNamesCache.getInvalidationCount();
    }

    @FixForMultiProject(description = "Cache keys and in-flight loads should be scoped by project")
    static final class DescriptorsAndApplicationNamesCache implements CacheInvalidatorRegistry.CacheInvalidator {
        private final Cache<String, Set<ApplicationPrivilegeDescriptor>> descriptorsCache;
        private final Cache<Set<String>, Set<String>> applicationNamesCache;
        private final LockingAtomicCounter lockingAtomicCounter;
        /**
         * Loads currently in flight. Concurrent cache misses for the same key join the existing load rather than each running a full
         * .security search, which would otherwise be repeated for every caller after a cache clear.
         */
        private final ConcurrentMap<InFlightKey, SubscribableListener<Collection<ApplicationPrivilegeDescriptor>>> inFlightLoads =
            new ConcurrentHashMap<>();
        /**
         * A shared load runs without the caller's security context or response headers. Each subscriber is completed on this executor
         * under its own captured context, so that one caller's security context never leaks into another's and a burst of callers does
         * not run all of its continuations serially on the one thread that delivered the response.
         */
        private final Executor completionExecutor;
        private final ThreadContext threadContext;

        DescriptorsAndApplicationNamesCache(TimeValue ttl, int cacheSize, Executor completionExecutor, ThreadContext threadContext) {
            this.descriptorsCache = CacheBuilder.<String, Set<ApplicationPrivilegeDescriptor>>builder()
                .setMaximumWeight(cacheSize)
                .weigher((k, v) -> v.size())
                .setExpireAfterWrite(ttl)
                .build();
            this.applicationNamesCache = CacheBuilder.<Set<String>, Set<String>>builder()
                .setMaximumWeight(cacheSize)
                .weigher((k, v) -> k.size() + v.size())
                .setExpireAfterWrite(ttl)
                .build();
            this.lockingAtomicCounter = new LockingAtomicCounter();
            this.completionExecutor = completionExecutor;
            this.threadContext = threadContext;
        }

        /**
         * Returns all privileges of the given application names, from the cache when it can answer for all of them and otherwise by
         * running {@code loader}, which must fetch all privileges of those applications. The loaded result is cached unless an
         * invalidation happened while it was loading.
         * <p>
         * Concurrent misses for the same key share one load. The key includes the invalidation count, so a caller that arrives after
         * an invalidation never joins a load that started before it.
         * <p>
         * A cache hit completes {@code listener} inline; otherwise every subscriber, the caller that starts the load included, is
         * completed on the completion executor under its own thread context (see {@link #completionExecutor}).
         */
        void get(
            Set<String> applicationNames,
            boolean waitForAvailableSecurityIndex,
            Consumer<ActionListener<Collection<ApplicationPrivilegeDescriptor>>> loader,
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
        ) {
            if (completeFromCache(applicationNames, listener)) {
                return;
            }

            final long invalidationCount = getInvalidationCount();
            final InFlightKey key = new InFlightKey(applicationNames, waitForAvailableSecurityIndex, invalidationCount);
            final SubscribableListener<Collection<ApplicationPrivilegeDescriptor>> newLoad = new SubscribableListener<>();
            final SubscribableListener<Collection<ApplicationPrivilegeDescriptor>> load = inFlightLoads.computeIfAbsent(key, k -> newLoad);
            load.addListener(listener, completionExecutor, threadContext);
            if (load != newLoad) {
                return;
            }

            // Remove the in-flight entry and complete the load in an empty context, so subscribers cannot inherit response headers
            // from the completing thread.
            final ActionListener<Collection<ApplicationPrivilegeDescriptor>> completionListener = new ContextPreservingActionListener<>(
                threadContext::newEmptyContext,
                ActionListener.runBefore(load, () -> inFlightLoads.remove(key, load))
            );
            // The load is shared, so run it detached from the caller that starts it. A stash, unlike an empty context, keeps the
            // project id, which the loader needs to resolve the project's security index.
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                // Catch exceptions from both the cache recheck and the loader so the shared load always completes and is removed.
                ActionListener.run(completionListener, l -> {
                    // Another load may have cached its result and removed its in-flight entry between the cache read above and the
                    // computeIfAbsent. Recheck now that this load is registered, so that callers that joined it are answered too.
                    if (completeFromCache(applicationNames, l)) {
                        return;
                    }

                    logger.debug("Fetching application privilege documents for: {}", applicationNames);
                    loader.accept(l.map(fetchedDescriptors -> {
                        // Cache before completion removes the in-flight entry and notifies subscribers.
                        putIfNoInvalidationSince(applicationNames, fetchedDescriptors, invalidationCount);
                        return fetchedDescriptors;
                    }));
                });
            }
        }

        /**
         * Completes {@code listener} with all privileges of the given application names if the cache can answer for all of them, and
         * returns whether it did. Returns {@code false}, leaving the listener untouched, if a load is needed. Wildcard names are never
         * answered from here, because the cache does not know what they expand to.
         */
        private boolean completeFromCache(
            Set<String> applicationNames,
            ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
        ) {
            // The application names cache also serves as a negative lookup: a non-wildcard name that does not exist maps to nothing
            final Set<String> concreteApplicationNames = applicationNamesCache.get(applicationNames);
            if (concreteApplicationNames != null && concreteApplicationNames.isEmpty()) {
                logger.debug("returning empty application privileges for [{}] as application names result in empty list", applicationNames);
                listener.onResponse(Set.of());
                return true;
            }
            final Set<ApplicationPrivilegeDescriptor> cachedDescriptors = new HashSet<>();
            for (String applicationName : concreteApplicationNames != null ? concreteApplicationNames : applicationNames) {
                if (applicationName.endsWith("*")) {
                    return false;
                }
                final Set<ApplicationPrivilegeDescriptor> descriptors = descriptorsCache.get(applicationName);
                if (descriptors == null) {
                    return false;
                }
                cachedDescriptors.addAll(descriptors);
            }
            logger.debug("All application privileges for [{}] found in cache", applicationNames);
            listener.onResponse(Collections.unmodifiableSet(cachedDescriptors));
            return true;
        }

        private record InFlightKey(Set<String> applicationNames, boolean waitForAvailableSecurityIndex, long invalidationCount) {}

        private void putIfNoInvalidationSince(
            Set<String> applicationNamesCacheKey,
            Collection<ApplicationPrivilegeDescriptor> fetchedDescriptors,
            long invalidationCount
        ) {
            final Map<String, Set<ApplicationPrivilegeDescriptor>> mapOfFetchedDescriptors = fetchedDescriptors.stream()
                .collect(Collectors.groupingBy(ApplicationPrivilegeDescriptor::getApplication, Collectors.toUnmodifiableSet()));
            lockingAtomicCounter.compareAndRun(invalidationCount, () -> {
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
            });
        }

        public long getInvalidationCount() {
            return lockingAtomicCounter.get();
        }

        public void invalidate(Collection<String> updatedApplicationNames) {
            lockingAtomicCounter.increment();
            logger.debug("Invalidating application privileges caches for: {}", updatedApplicationNames);
            final Set<String> uniqueNames = Set.copyOf(updatedApplicationNames);
            // Always completely invalidate application names cache due to wildcard
            applicationNamesCache.invalidateAll();
            updatedApplicationNames.forEach(descriptorsCache::invalidate);
        }

        public void invalidateAll() {
            lockingAtomicCounter.increment();
            logger.debug("Invalidating all application privileges caches");
            applicationNamesCache.invalidateAll();
            descriptorsCache.invalidateAll();
        }
    }
}
