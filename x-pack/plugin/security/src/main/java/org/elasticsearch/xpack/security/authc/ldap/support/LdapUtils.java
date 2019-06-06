/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.AsyncRequestID;
import com.unboundid.ldap.sdk.AsyncSearchResultListener;
import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.DereferencePolicy;
import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.SearchRequest;
import com.unboundid.ldap.sdk.SearchResult;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchResultReference;
import com.unboundid.ldap.sdk.SearchScope;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.support.Exceptions;

import javax.naming.ldap.Rdn;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public final class LdapUtils {

    public static final Filter OBJECT_CLASS_PRESENCE_FILTER = Filter.createPresenceFilter("objectClass");

    private static final Logger LOGGER = LogManager.getLogger(LdapUtils.class);

    private LdapUtils() {
    }

    public static DN dn(String dn) {
        try {
            return new DN(dn);
        } catch (LDAPException e) {
            throw new IllegalArgumentException("invalid DN [" + dn + "]", e);
        }
    }

    public static <T> T privilegedConnect(CheckedSupplier<T, LDAPException> supplier)
            throws LDAPException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) supplier::get);
        } catch (PrivilegedActionException e) {
            throw (LDAPException) e.getCause();
        }
    }

    public static String relativeName(DN dn) {
        return dn.getRDNString().split("=")[1].trim();
    }

    public static String escapedRDNValue(String rdn) {
        // We can't use UnboundID RDN here because it expects attribute=value, not just value
        return Rdn.escapeValue(rdn);
    }

    /**
     * If necessary, fork before executing the runnable. A deadlock will happen if
     * the same thread which handles bind responses blocks on the bind call, waiting
     * for the response which he itself should handle.
     */
    private static void maybeForkAndRun(ThreadPool threadPool, Runnable runnable) {
        if (isLdapConnectionThread(Thread.currentThread())) {
            // only fork if binding on the LDAPConnectionReader thread
            threadPool.executor(ThreadPool.Names.GENERIC).execute(runnable);
        } else {
            // avoids repeated forking
            runnable.run();
        }
    }

    /**
     * This method submits the {@code bind} request over one connection from the
     * pool. The bind authentication is then reverted and the connection is returned
     * to the pool, so that the connection can be safely reused, see
     * {@code LDAPConnectionPool#bindAndRevertAuthentication}. This validates the
     * bind credentials.
     *
     * Bind calls are blocking and if a bind is executed on the LDAP Connection
     * Reader thread (as returned by {@code LdapUtils#isLdapConnectionThread}), the
     * thread will be blocked until it is interrupted by something else such as a
     * timeout timer. <b>Do not call bind</b> outside this method or
     * {@link LdapUtils#maybeForkThenBind(LDAPConnection, BindRequest, ThreadPool, AbstractRunnable)}
     *
     * @param ldapPool
     *            The LDAP connection pool on which to submit the bind operation.
     * @param bind
     *            The request object of the bind operation.
     * @param threadPool
     *            The threads that will call the blocking bind operation, in case
     *            the calling thread is a connection reader, see:
     *            {@code LdapUtils#isLdapConnectionThread}.
     * @param runnable
     *            The runnable that continues the program flow after the bind
     *            operation. It is executed on the same thread as the prior bind.
     */
    public static void maybeForkThenBindAndRevert(LDAPConnectionPool ldapPool, BindRequest bind, ThreadPool threadPool,
                                                  AbstractRunnable runnable) {
        final Runnable bindRunnable = new AbstractRunnable() {
            @Override
            @SuppressForbidden(reason = "Bind allowed if forking of the LDAP Connection Reader Thread.")
            protected void doRun() throws Exception {
                privilegedConnect(() -> ldapPool.bindAndRevertAuthentication(bind.duplicate()));
                LOGGER.trace("LDAP bind [{}] succeeded for [{}]", bind, ldapPool);
                runnable.run();
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.debug("LDAP bind [{}] failed for [{}] - [{}]", bind, ldapPool, e.toString());
                runnable.onFailure(e);
            }

            @Override
            public void onAfter() {
                runnable.onAfter();
            }
        };
        maybeForkAndRun(threadPool, bindRunnable);
    }

    /**
     * This method submits the {@code bind} request over the ldap connection. Its
     * authentication status changes. The connection can be subsequently reused.
     * This validates the bind credentials.
     *
     * Bind calls are blocking and if a bind is executed on the LDAP Connection
     * Reader thread (as returned by {@code LdapUtils#isLdapConnectionThread}), the
     * thread will be blocked until it is interrupted by something else such as a
     * timeout timer. <b>Do not call bind</b> outside this method or
     * {@link LdapUtils#maybeForkThenBind(LDAPConnection, BindRequest, ThreadPool, AbstractRunnable)}
     *
     * @param ldap
     *            The LDAP connection on which to submit the bind operation.
     * @param bind
     *            The request object of the bind operation.
     * @param threadPool
     *            The threads that will call the blocking bind operation, in case
     *            the calling thread is a connection reader, see:
     *            {@code LdapUtils#isLdapConnectionThread}.
     * @param runnable
     *            The runnable that continues the program flow after the bind
     *            operation. It is executed on the same thread as the prior bind.
     */
    public static void maybeForkThenBind(LDAPConnection ldap, BindRequest bind, ThreadPool threadPool, AbstractRunnable runnable) {
        final Runnable bindRunnable = new AbstractRunnable() {
            @Override
            @SuppressForbidden(reason = "Bind allowed if forking of the LDAP Connection Reader Thread.")
            protected void doRun() throws Exception {
                privilegedConnect(() -> ldap.bind(bind.duplicate()));
                LOGGER.trace("LDAP bind [{}] succeeded for [{}]", bind, ldap);
                runnable.run();
            }

            @Override
            public void onFailure(Exception e) {
                LOGGER.debug("LDAP bind [{}] failed for [{}] - [{}]", bind, ldap, e.toString());
                runnable.onFailure(e);
            }

            @Override
            public void onAfter() {
                runnable.onAfter();
            }
        };
        maybeForkAndRun(threadPool, bindRunnable);
    }

    /**
     * This method performs an asynchronous ldap search operation that could have multiple results
     */
    public static void searchForEntry(LDAPInterface ldap, String baseDN, SearchScope scope,
                                      Filter filter, int timeLimitSeconds,
                                      boolean ignoreReferralErrors,
                                      ActionListener<SearchResultEntry> listener,
                                      String... attributes) {
        if (ldap instanceof LDAPConnection) {
            searchForEntry((LDAPConnection) ldap, baseDN, scope, filter, timeLimitSeconds,
                    ignoreReferralErrors, listener, attributes);
        } else if (ldap instanceof LDAPConnectionPool) {
            searchForEntry((LDAPConnectionPool) ldap, baseDN, scope, filter, timeLimitSeconds,
                    ignoreReferralErrors, listener, attributes);
        } else {
            throw new IllegalArgumentException("unsupported LDAPInterface implementation: " + ldap);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that only expects at most one
     * result.
     * If more than one result is found then this is an error
     * If no results are found, then {@code null} will be returned.
     * If the LDAP server returns an error {@link ResultCode} then this is handled as a
     * {@link ActionListener#onFailure(Exception) failure}
     */
    public static void searchForEntry(LDAPConnection ldap, String baseDN, SearchScope scope,
                                      Filter filter, int timeLimitSeconds,
                                      boolean ignoreReferralErrors,
                                      ActionListener<SearchResultEntry> listener,
                                      String... attributes) {
        LdapSearchResultListener searchResultListener = new SingleEntryListener(ldap, listener,
                filter, ignoreReferralErrors);
        try {
            SearchRequest request = new SearchRequest(searchResultListener, baseDN, scope,
                    DereferencePolicy.NEVER, 0, timeLimitSeconds, false, filter, attributes);
            searchResultListener.setSearchRequest(request);
            ldap.asyncSearch(request);
        } catch (LDAPException e) {
            listener.onFailure(e);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that only expects at most one
     * result.
     * If more than one result is found then this is an error.
     * If no results are found, then {@code null} will be returned.
     * If the LDAP server returns an error {@link ResultCode} then this is handled as a
     * {@link ActionListener#onFailure(Exception) failure}
     */
    public static void searchForEntry(LDAPConnectionPool ldap, String baseDN, SearchScope scope,
                                      Filter filter, int timeLimitSeconds,
                                      boolean ignoreReferralErrors,
                                      ActionListener<SearchResultEntry> listener,
                                      String... attributes) {
        boolean searching = false;
        LDAPConnection ldapConnection = null;
        try {
            ldapConnection = privilegedConnect(ldap::getConnection);
            final LDAPConnection finalConnection = ldapConnection;
            searchForEntry(finalConnection, baseDN, scope, filter, timeLimitSeconds,
                    ignoreReferralErrors, ActionListener.wrap(
                            entry -> {
                                assert isLdapConnectionThread(Thread.currentThread()) : "Expected current thread [" + Thread.currentThread()
                                        + "] to be an LDAPConnectionReader Thread. Probably the new library has changed the thread's name.";
                                IOUtils.close(() -> ldap.releaseConnection(finalConnection));
                                listener.onResponse(entry);
                            },
                            e -> {
                                IOUtils.closeWhileHandlingException(
                                        () -> ldap.releaseConnection(finalConnection)
                                );
                                listener.onFailure(e);
                            }), attributes);
            searching = true;
        } catch (LDAPException e) {
            listener.onFailure(e);
        } finally {
            if (searching == false) {
                final LDAPConnection finalConnection = ldapConnection;
                IOUtils.closeWhileHandlingException(() -> ldap.releaseConnection(finalConnection));
            }
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that could have multiple results
     */
    public static void search(LDAPInterface ldap, String baseDN, SearchScope scope,
                              Filter filter, int timeLimitSeconds,
                              boolean ignoreReferralErrors,
                              ActionListener<List<SearchResultEntry>> listener,
                              String... attributes) {
        if (ldap instanceof LDAPConnection) {
            search((LDAPConnection) ldap, baseDN, scope, filter, timeLimitSeconds,
                    ignoreReferralErrors, listener, attributes);
        } else if (ldap instanceof LDAPConnectionPool) {
            search((LDAPConnectionPool) ldap, baseDN, scope, filter, timeLimitSeconds,
                    ignoreReferralErrors, listener, attributes);
        } else {
            throw new IllegalArgumentException("unsupported LDAPInterface implementation: " + ldap);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that could have multiple results
     */
    public static void search(LDAPConnection ldap, String baseDN, SearchScope scope,
                              Filter filter, int timeLimitSeconds,
                              boolean ignoreReferralErrors,
                              ActionListener<List<SearchResultEntry>> listener,
                              String... attributes) {
        LdapSearchResultListener searchResultListener = new LdapSearchResultListener(
                ldap,
                ignoreReferralErrors,
                ActionListener.wrap(
                        searchResult -> {
                            assert isLdapConnectionThread(Thread.currentThread()) : "Expected current thread [" + Thread.currentThread()
                                    + "] to be an LDAPConnectionReader Thread. Probably the new library has changed the thread's name.";
                            listener.onResponse(Collections.unmodifiableList(searchResult.getSearchEntries()));
                        },
                        listener::onFailure),
                1);
        try {
            SearchRequest request = new SearchRequest(searchResultListener, baseDN, scope,
                    DereferencePolicy.NEVER, 0, timeLimitSeconds, false, filter, attributes);
            searchResultListener.setSearchRequest(request);
            ldap.asyncSearch(request);
        } catch (LDAPException e) {
            listener.onFailure(e);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that could have multiple results
     */
    public static void search(LDAPConnectionPool ldap, String baseDN, SearchScope scope,
                              Filter filter, int timeLimitSeconds,
                              boolean ignoreReferralErrors,
                              ActionListener<List<SearchResultEntry>> listener,
                              String... attributes) {
        boolean searching = false;
        LDAPConnection ldapConnection = null;
        try {
            ldapConnection = privilegedConnect(ldap::getConnection);
            final LDAPConnection finalConnection = ldapConnection;
            search(finalConnection, baseDN, scope, filter, timeLimitSeconds, ignoreReferralErrors, ActionListener.wrap(searchResult -> {
                IOUtils.closeWhileHandlingException(() -> ldap.releaseConnection(finalConnection));
                listener.onResponse(searchResult);
            }, (e) -> {
                IOUtils.closeWhileHandlingException(() -> ldap.releaseConnection(finalConnection));
                listener.onFailure(e);
            }), attributes);
            searching = true;
        } catch (LDAPException e) {
            listener.onFailure(e);
        } finally {
            if (searching == false && ldapConnection != null) {
                final LDAPConnection finalConnection = ldapConnection;
                IOUtils.closeWhileHandlingException(() -> ldap.releaseConnection(finalConnection));
            }
        }
    }

    static boolean isLdapConnectionThread(Thread thread) {
        return Thread.currentThread().getName().startsWith("Connection reader for connection ");
    }

    /**
     * Returns <code>true</code> if the provide {@link SearchResult} was successfully completed
     * by the server.
     * <strong>Note:</strong> Referrals are <em>not</em> considered a successful response for the
     * purposes of this method.
     */
    private static boolean isSuccess(SearchResult searchResult) {
        switch (searchResult.getResultCode().intValue()) {
            case ResultCode.SUCCESS_INT_VALUE:
            case ResultCode.COMPARE_FALSE_INT_VALUE:
            case ResultCode.COMPARE_TRUE_INT_VALUE:
                return true;
            default:
                return false;
        }
    }

    private static SearchResult emptyResult(SearchResult parentResult) {
        return new SearchResult(
                parentResult.getMessageID(),
                ResultCode.SUCCESS,
                "Empty result",
                parentResult.getMatchedDN(),
                null,
                0,
                0,
                null
        );
    }

    private static LDAPException toException(SearchResult searchResult) {
        return new LDAPException(
                searchResult.getResultCode(),
                searchResult.getDiagnosticMessage(),
                searchResult.getMatchedDN(),
                searchResult.getReferralURLs(),
                searchResult.getResponseControls()
        );
    }

    public static Filter createFilter(String filterTemplate, String... arguments) throws LDAPException {
        return Filter.create(new MessageFormat(filterTemplate, Locale.ROOT)
                .format(encodeFilterValues(arguments), new StringBuffer(), null)
                .toString());
    }

    public static String[] attributesToSearchFor(String[] attributes) {
        return attributes == null ? new String[] { SearchRequest.NO_ATTRIBUTES } : attributes;
    }

    public static String[] attributesToSearchFor(String[]... args) {
        List<String> attributes = new ArrayList<>();
        for (String[] array : args) {
            if (array != null) {
                attributes.addAll(Arrays.asList(array));
            }
        }
        return attributes.isEmpty() ? attributesToSearchFor((String[]) null)
                : attributes.toArray(new String[attributes.size()]);
    }

    private static String[] encodeFilterValues(String... arguments) {
        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = Filter.encodeValue(arguments[i]);
        }
        return arguments;
    }

    private static class SingleEntryListener extends LdapSearchResultListener {

        SingleEntryListener(LDAPConnection ldapConnection,
                            ActionListener<SearchResultEntry> listener, Filter filter,
                            boolean ignoreReferralErrors) {
            super(ldapConnection, ignoreReferralErrors, ActionListener.wrap(searchResult -> {
                        final List<SearchResultEntry> entryList = searchResult.getSearchEntries();
                        if (entryList.size() > 1) {
                            listener.onFailure(Exceptions.authenticationError(
                                    "multiple search results found for [{}]", filter));
                        } else if (entryList.size() == 1) {
                            listener.onResponse(entryList.get(0));
                        } else {
                            listener.onResponse(null);
                        }
                    }, listener::onFailure)
                    , 1);
        }
    }

    private static class LdapSearchResultListener implements AsyncSearchResultListener {

        private final List<SearchResultEntry> entryList = new ArrayList<>();
        private final List<SearchResultReference> referenceList = new ArrayList<>();
        protected final SetOnce<SearchRequest> searchRequestRef = new SetOnce<>();

        private final LDAPConnection ldapConnection;
        private final boolean ignoreReferralErrors;
        private final ActionListener<SearchResult> listener;
        private final int depth;

        LdapSearchResultListener(LDAPConnection ldapConnection, boolean ignoreReferralErrors,
                                 ActionListener<SearchResult> listener, int depth) {
            this.ldapConnection = ldapConnection;
            this.listener = listener;
            this.depth = depth;
            this.ignoreReferralErrors = ignoreReferralErrors;
        }

        @Override
        public void searchEntryReturned(SearchResultEntry searchEntry) {
            entryList.add(searchEntry);
        }

        @Override
        public void searchReferenceReturned(SearchResultReference searchReference) {
            referenceList.add(searchReference);
        }

        @Override
        public void searchResultReceived(AsyncRequestID requestID, SearchResult searchResult) {
            // Whenever we get a search result we need to check for a referral.
            // A referral is a mechanism for an LDAP server to reference an object stored in a
            // different LDAP server/partition. There are cases where we need to follow a referral
            // in order to get the actual object we are searching for
            final String[] referralUrls = referenceList.stream()
                    .flatMap((ref) -> Arrays.stream(ref.getReferralURLs()))
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY);
            final SearchRequest request = searchRequestRef.get();
            if (referralUrls.length == 0 || request.followReferrals(ldapConnection) == false) {
                // either no referrals to follow or we have explicitly disabled referral following
                // on the connection so we just create a new search result that has the values we've
                // collected. The search result passed to this method will not have of the entries
                // as we are using a result listener and the results are not being collected by the
                // LDAP library
                LOGGER.trace("LDAP Search {} => {} ({})", request, searchResult, entryList);
                if (isSuccess(searchResult)) {
                    SearchResult resultWithValues = new SearchResult(searchResult.getMessageID(),
                            searchResult.getResultCode(), searchResult.getDiagnosticMessage(),
                            searchResult.getMatchedDN(), referralUrls, entryList, referenceList,
                            entryList.size(), referenceList.size(),
                            searchResult.getResponseControls());
                    listener.onResponse(resultWithValues);
                } else {
                    listener.onFailure(toException(searchResult));
                }
            } else if (depth >= ldapConnection.getConnectionOptions().getReferralHopLimit()) {
                // we've gone through too many levels of referrals so we terminate with the values
                // collected so far and the proper result code to indicate the search was
                // terminated early
                LOGGER.trace("Referral limit exceeded {} => {} ({})",
                        request, searchResult, entryList);
                listener.onFailure(new LDAPException(ResultCode.REFERRAL_LIMIT_EXCEEDED,
                        "Referral limit exceeded (" + depth + ")",
                        searchResult.getMatchedDN(), referralUrls,
                        searchResult.getResponseControls()));
            } else {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("LDAP referred elsewhere {} => {}",
                            request, Arrays.toString(referralUrls));
                }
                // there are referrals to follow, so we start the process to follow the referrals
                final CountDown countDown = new CountDown(referralUrls.length);
                final List<String> referralUrlsList = new ArrayList<>(Arrays.asList(referralUrls));

                ActionListener<SearchResult> referralListener = ActionListener.wrap(
                        innerResult -> {
                            // synchronize here since we are possibly sending out a lot of requests
                            // and the result lists are not thread safe and this also provides us
                            // with a consistent view
                            synchronized (this) {
                                if (innerResult.getSearchEntries() != null) {
                                    entryList.addAll(innerResult.getSearchEntries());
                                }
                                if (innerResult.getSearchReferences() != null) {
                                    referenceList.addAll(innerResult.getSearchReferences());
                                }
                            }

                            // count down and once all referrals have been traversed then we can
                            // create the results
                            if (countDown.countDown()) {
                                SearchResult resultWithValues = new SearchResult(
                                        searchResult.getMessageID(), searchResult.getResultCode(),
                                        searchResult.getDiagnosticMessage(),
                                        searchResult.getMatchedDN(),
                                        referralUrlsList.toArray(Strings.EMPTY_ARRAY), entryList,
                                        referenceList, entryList.size(), referenceList.size(),
                                        searchResult.getResponseControls());
                                listener.onResponse(resultWithValues);
                            }
                        }, listener::onFailure);

                for (String referralUrl : referralUrls) {
                    try {
                        // for each referral follow it and any other referrals returned until we
                        // get to a depth that is greater than or equal to the referral hop limit
                        // or all referrals have been followed. Each time referrals are followed
                        // from a search result, the depth increases by 1
                        followReferral(ldapConnection, referralUrl, request, referralListener,
                                depth + 1, ignoreReferralErrors, searchResult);
                    } catch (LDAPException e) {
                        LOGGER.warn((Supplier<?>) () -> new ParameterizedMessage(
                                "caught exception while trying to follow referral [{}]",
                                referralUrl), e);
                        if (ignoreReferralErrors) {
                            // Needed in order for the countDown to be correct
                            referralListener.onResponse(emptyResult(searchResult));
                        } else {
                            listener.onFailure(e);
                        }
                    }
                }
            }
        }

        void setSearchRequest(SearchRequest searchRequest) {
            this.searchRequestRef.set(searchRequest);
        }
    }

    /**
     * Performs the actual connection and following of a referral given a URL string.
     * This referral is being followed as it may contain a result that is relevant to our search
     */
    private static void followReferral(LDAPConnection ldapConnection, String urlString,
                                       SearchRequest searchRequest,
                                       ActionListener<SearchResult> listener, int depth,
                                       boolean ignoreErrors, SearchResult originatingResult)
            throws LDAPException {

        final LDAPURL referralURL = new LDAPURL(urlString);
        final String host = referralURL.getHost();
        // the host must be present in order to follow a referral
        if (host == null) {
            // nothing to really do since a null host cannot really be handled, so we treat it as
            // an error
            throw new LDAPException(ResultCode.UNAVAILABLE, "Null referral host in " + urlString);
        }

        // the referral URL often contains information necessary about the LDAP request such as
        // the base DN, scope, and filter. If it does not, then we reuse the values from the
        // originating search request
        final String requestBaseDN;
        if (referralURL.baseDNProvided()) {
            requestBaseDN = referralURL.getBaseDN().toString();
        } else {
            requestBaseDN = searchRequest.getBaseDN();
        }

        final SearchScope requestScope;
        if (referralURL.scopeProvided()) {
            requestScope = referralURL.getScope();
        } else {
            requestScope = searchRequest.getScope();
        }

        final Filter requestFilter;
        if (referralURL.filterProvided()) {
            requestFilter = referralURL.getFilter();
        } else {
            requestFilter = searchRequest.getFilter();
        }

        // in order to follow the referral we need to open a new connection and we do so using the
        // referral connector on the ldap connection
        final LDAPConnection referralConn =
                privilegedConnect(() -> ldapConnection.getReferralConnector().getReferralConnection(referralURL, ldapConnection));
        final LdapSearchResultListener ldapListener = new LdapSearchResultListener(
                referralConn, ignoreErrors,
                ActionListener.wrap(
                        searchResult -> {
                            IOUtils.close(referralConn);
                            listener.onResponse(searchResult);
                        },
                        e -> {
                            IOUtils.closeWhileHandlingException(referralConn);
                            if (ignoreErrors) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(new ParameterizedMessage(
                                            "Failed to retrieve results from referral URL [{}]." +
                                                    " Treating as 'no results'",
                                            referralURL), e);
                                }
                                listener.onResponse(emptyResult(originatingResult));
                            } else {
                                listener.onFailure(e);
                            }
                        }),
                depth);
        boolean success = false;
        try {
            final SearchRequest referralSearchRequest =
                    new SearchRequest(ldapListener, searchRequest.getControls(),
                            requestBaseDN, requestScope, searchRequest.getDereferencePolicy(),
                            searchRequest.getSizeLimit(), searchRequest.getTimeLimitSeconds(),
                            searchRequest.typesOnly(), requestFilter,
                            searchRequest.getAttributes());
            ldapListener.setSearchRequest(searchRequest);
            referralConn.asyncSearch(referralSearchRequest);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(referralConn);
            }
        }
    }
}
