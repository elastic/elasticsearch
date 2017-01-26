/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.AsyncRequestID;
import com.unboundid.ldap.sdk.AsyncSearchResultListener;
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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.xpack.security.support.Exceptions;

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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public final class LdapUtils {

    public static final Filter OBJECT_CLASS_PRESENCE_FILTER = Filter.createPresenceFilter("objectClass");

    private LdapUtils() {
    }

    public static DN dn(String dn) {
        try {
            return new DN(dn);
        } catch (LDAPException e) {
            throw new IllegalArgumentException("invalid DN [" + dn + "]", e);
        }
    }

    public static <T> T privilegedConnect(CheckedSupplier<T, LDAPException> supplier) throws LDAPException {
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
     * This method performs an asynchronous ldap search operation that could have multiple results
     */
    public static void searchForEntry(LDAPInterface ldap, String baseDN, SearchScope scope, Filter filter, int timeLimitSeconds,
                                      ActionListener<SearchResultEntry> listener, String... attributes) {
        if (ldap instanceof LDAPConnection) {
            searchForEntry((LDAPConnection) ldap, baseDN, scope, filter, timeLimitSeconds, listener, attributes);
        } else if (ldap instanceof LDAPConnectionPool) {
            searchForEntry((LDAPConnectionPool) ldap, baseDN, scope, filter, timeLimitSeconds, listener, attributes);
        } else {
            throw new IllegalArgumentException("unsupported LDAPInterface implementation: " + ldap);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that only expects at most one result. If more than one result is found
     * then this is an error. If no results are found, then {@code null} will be returned.
     */
    public static void searchForEntry(LDAPConnection ldap, String baseDN, SearchScope scope, Filter filter, int timeLimitSeconds,
                                      ActionListener<SearchResultEntry> listener, String... attributes) {
        LdapSearchResultListener searchResultListener = new SingleEntryListener(ldap, listener, filter);
        try {
            SearchRequest request = new SearchRequest(searchResultListener, baseDN, scope, DereferencePolicy.NEVER, 0, timeLimitSeconds,
                    false, filter, attributes);
            searchResultListener.setSearchRequest(request);
            ldap.asyncSearch(request);
        } catch (LDAPException e) {
            listener.onFailure(e);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that only expects at most one result. If more than one result is found
     * then this is an error. If no results are found, then {@code null} will be returned.
     */
    public static void searchForEntry(LDAPConnectionPool ldap, String baseDN, SearchScope scope, Filter filter, int timeLimitSeconds,
                                      ActionListener<SearchResultEntry> listener, String... attributes) {
        boolean searching = false;
        LDAPConnection ldapConnection = null;
        try {
            ldapConnection = privilegedConnect(ldap::getConnection);
            final LDAPConnection finalConnection = ldapConnection;
            searchForEntry(finalConnection, baseDN, scope, filter, timeLimitSeconds, ActionListener.wrap(
                    (entry) -> {
                        IOUtils.close(() -> ldap.releaseConnection(finalConnection));
                        listener.onResponse(entry);
                    },
                    (e) -> {
                        IOUtils.closeWhileHandlingException(() -> ldap.releaseConnection(finalConnection));
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
    public static void search(LDAPInterface ldap, String baseDN, SearchScope scope, Filter filter, int timeLimitSeconds,
                              ActionListener<List<SearchResultEntry>> listener, String... attributes) {
        if (ldap instanceof LDAPConnection) {
            search((LDAPConnection) ldap, baseDN, scope, filter, timeLimitSeconds, listener, attributes);
        } else if (ldap instanceof LDAPConnectionPool) {
            search((LDAPConnectionPool) ldap, baseDN, scope, filter, timeLimitSeconds, listener, attributes);
        } else {
            throw new IllegalArgumentException("unsupported LDAPInterface implementation: " + ldap);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that could have multiple results
     */
    public static void search(LDAPConnection ldap, String baseDN, SearchScope scope, Filter filter, int timeLimitSeconds,
                                      ActionListener<List<SearchResultEntry>> listener, String... attributes) {
        LdapSearchResultListener searchResultListener = new LdapSearchResultListener(ldap,
                (asyncRequestID, searchResult) -> listener.onResponse(Collections.unmodifiableList(searchResult.getSearchEntries())), 1);

        try {
            SearchRequest request = new SearchRequest(searchResultListener, baseDN, scope, DereferencePolicy.NEVER, 0, timeLimitSeconds,
                    false, filter, attributes);
            searchResultListener.setSearchRequest(request);
            ldap.asyncSearch(request);
        } catch (LDAPException e) {
            listener.onFailure(e);
        }
    }

    /**
     * This method performs an asynchronous ldap search operation that could have multiple results
     */
    public static void search(LDAPConnectionPool ldap, String baseDN, SearchScope scope, Filter filter, int timeLimitSeconds,
                              ActionListener<List<SearchResultEntry>> listener, String... attributes) {
        boolean searching = false;
        LDAPConnection ldapConnection = null;
        try {
            ldapConnection = ldap.getConnection();
            final LDAPConnection finalConnection = ldapConnection;
            LdapSearchResultListener ldapSearchResultListener = new LdapSearchResultListener(ldapConnection,
                    (asyncRequestID, searchResult) -> {
                        IOUtils.closeWhileHandlingException(() -> ldap.releaseConnection(finalConnection));
                        listener.onResponse(Collections.unmodifiableList(searchResult.getSearchEntries()));
                    }, 1);
            SearchRequest request = new SearchRequest(ldapSearchResultListener, baseDN, scope, DereferencePolicy.NEVER, 0, timeLimitSeconds,
                    false, filter, attributes);
            ldapSearchResultListener.setSearchRequest(request);
            finalConnection.asyncSearch(request);
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

    public static Filter createFilter(String filterTemplate, String... arguments) throws LDAPException {
        return Filter.create(new MessageFormat(filterTemplate, Locale.ROOT).format((Object[]) encodeFilterValues(arguments),
                new StringBuffer(), null).toString());
    }

    public static String[] attributesToSearchFor(String[] attributes) {
        return attributes == null ? new String[] { SearchRequest.NO_ATTRIBUTES } : attributes;
    }

    static String[] encodeFilterValues(String... arguments) {
        for (int i = 0; i < arguments.length; i++) {
            arguments[i] = Filter.encodeValue(arguments[i]);
        }
        return arguments;
    }

    private static class SingleEntryListener extends LdapSearchResultListener {

        SingleEntryListener(LDAPConnection ldapConnection, ActionListener<SearchResultEntry> listener, Filter filter) {
            super(ldapConnection, ((asyncRequestID, searchResult) -> {
                final List<SearchResultEntry> entryList = searchResult.getSearchEntries();
                if (entryList.size() > 1) {
                    listener.onFailure(Exceptions.authenticationError("multiple search results found for [{}]", filter));
                } else if (entryList.size() == 1) {
                    listener.onResponse(entryList.get(0));
                } else {
                    listener.onResponse(null);
                }
            }), 1);
        }

    }

    private static class LdapSearchResultListener implements AsyncSearchResultListener {
        private static final Logger LOGGER = ESLoggerFactory.getLogger(LdapUtils.class);

        private final List<SearchResultEntry> entryList = new ArrayList<>();
        private final List<SearchResultReference> referenceList = new ArrayList<>();
        protected final SetOnce<SearchRequest> searchRequestRef = new SetOnce<>();
        private final BiConsumer<AsyncRequestID, SearchResult> consumer;
        private final LDAPConnection ldapConnection;
        private final int depth;

        LdapSearchResultListener(LDAPConnection ldapConnection, BiConsumer<AsyncRequestID, SearchResult> consumer, int depth) {
            this.ldapConnection = ldapConnection;
            this.consumer = consumer;
            this.depth = depth;
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
            // whenever we get a search result we need to check for a referral. A referral is a mechanism for an LDAP server to reference
            // an object stored in a different LDAP server/partition. There are cases where we need to follow a referral in order to get
            // the actual object we are searching for
            final String[] referralUrls = referenceList.stream()
                    .flatMap((ref) -> Arrays.stream(ref.getReferralURLs()))
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY);
            final SearchRequest searchRequest = searchRequestRef.get();
            if (referralUrls.length == 0 || searchRequest.followReferrals(ldapConnection) == false) {
                // either no referrals to follow or we have explicitly disabled referral following on the connection so we just create
                // a new search result that has the values we've collected. The search result passed to this method will not have of the
                // entries as we are using a result listener and the results are not being collected by the LDAP library
                LOGGER.trace("LDAP Search {} => {} ({})", searchRequest, searchResult, entryList);
                SearchResult resultWithValues = new SearchResult(searchResult.getMessageID(), searchResult.getResultCode(), searchResult
                        .getDiagnosticMessage(), searchResult.getMatchedDN(), referralUrls, entryList, referenceList, entryList.size(),
                        referenceList.size(), searchResult.getResponseControls());
                consumer.accept(requestID, resultWithValues);
            } else if (depth >= ldapConnection.getConnectionOptions().getReferralHopLimit()) {
                // we've gone through too many levels of referrals so we terminate with the values collected so far and the proper result
                // code to indicate the search was terminated early
                LOGGER.trace("Referral limit exceeded {} => {} ({})", searchRequest, searchResult, entryList);
                SearchResult resultWithValues = new SearchResult(searchResult.getMessageID(), ResultCode.REFERRAL_LIMIT_EXCEEDED,
                        searchResult.getDiagnosticMessage(), searchResult.getMatchedDN(), referralUrls, entryList, referenceList,
                        entryList.size(), referenceList.size(), searchResult.getResponseControls());
                consumer.accept(requestID, resultWithValues);
            } else {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("LDAP referred elsewhere {} => {}", searchRequest, Arrays.toString(referralUrls));
                }
                // there are referrals to follow, so we start the process to follow the referrals
                final CountDown countDown = new CountDown(referralUrls.length);
                final List<String> referralUrlsList = new ArrayList<>(Arrays.asList(referralUrls));

                BiConsumer<AsyncRequestID, SearchResult> referralConsumer = (reqID, innerResult) -> {
                    // synchronize here since we are possibly sending out a lot of requests and the result lists are not thread safe and
                    // this also provides us with a consistent view
                    synchronized (this) {
                        if (innerResult.getSearchEntries() != null) {
                            entryList.addAll(innerResult.getSearchEntries());
                        }
                        if (innerResult.getSearchReferences() != null) {
                            referenceList.addAll(innerResult.getSearchReferences());
                        }
                    }

                    // count down and once all referrals have been traversed then we can create the results
                    if (countDown.countDown()) {
                        SearchResult resultWithValues = new SearchResult(searchResult.getMessageID(), searchResult.getResultCode(),
                                searchResult.getDiagnosticMessage(), searchResult.getMatchedDN(),
                                referralUrlsList.toArray(Strings.EMPTY_ARRAY), entryList, referenceList,
                                entryList.size(), referenceList.size(), searchResult.getResponseControls());
                        consumer.accept(requestID, resultWithValues);
                    }
                };

                for (String referralUrl : referralUrls) {
                    try {
                        // for each referral follow it and any other referrals returned until we get to a depth that is greater than or
                        // equal to the referral hop limit or all referrals have been followed. Each time referrals are followed from a
                        // search result, the depth increases by 1
                        followReferral(ldapConnection, referralUrl, searchRequest, referralConsumer, depth + 1, searchResult, requestID);
                    } catch (LDAPException e) {
                        LOGGER.warn((Supplier<?>)
                                () -> new ParameterizedMessage("caught exception while trying to follow referral [{}]", referralUrl), e);
                        referralConsumer.accept(requestID, new SearchResult(searchResult.getMessageID(), e.getResultCode(),
                                e.getDiagnosticMessage(), e.getMatchedDN(), e.getReferralURLs(), 0, 0, e.getResponseControls()));
                    }
                }
            }
        }

        void setSearchRequest(SearchRequest searchRequest) {
            this.searchRequestRef.set(searchRequest);
        }
    }

    /**
     * Performs the actual connection and following of a referral given a URL string. This referral is being followed as it may contain a
     * result that is relevant to our search
     */
    private static void followReferral(LDAPConnection ldapConnection, String urlString, SearchRequest searchRequest,
                                       BiConsumer<AsyncRequestID, SearchResult> consumer, int depth,
                                       SearchResult originatingResult, AsyncRequestID asyncRequestID) throws LDAPException {
        final LDAPURL referralURL = new LDAPURL(urlString);
        final String host = referralURL.getHost();

        // the host must be present in order to follow a referral
        if (host != null) {
            // the referral URL often contains information necessary about the LDAP request such as the base DN, scope, and filter. If it
            // does not, then we reuse the values from the originating search request
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

            // in order to follow the referral we need to open a new connection and we do so using the referral connector on the ldap
            // connection
            final LDAPConnection referralConn =
                    ldapConnection.getReferralConnector().getReferralConnection(referralURL, ldapConnection);
            final LdapSearchResultListener listener = new LdapSearchResultListener(referralConn,
                    (reqId, searchResult) -> {
                        IOUtils.closeWhileHandlingException(referralConn);
                        consumer.accept(reqId, searchResult);
                    }, depth);
            boolean success = false;
            try {
                final SearchRequest referralSearchRequest =
                        new SearchRequest(listener, searchRequest.getControls(),
                                requestBaseDN, requestScope, searchRequest.getDereferencePolicy(),
                                searchRequest.getSizeLimit(), searchRequest.getTimeLimitSeconds(), searchRequest.typesOnly(),
                                requestFilter, searchRequest.getAttributes());
                listener.setSearchRequest(searchRequest);
                referralConn.asyncSearch(referralSearchRequest);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(referralConn);
                }
            }
        } else {
            // nothing to really do since a null host cannot really be handled, so we just return with a response that is empty...
            consumer.accept(asyncRequestID, new SearchResult(originatingResult.getMessageID(), ResultCode.UNAVAILABLE,
                    null, null, null, Collections.emptyList(), Collections.emptyList(), 0, 0, null));
        }
    }
}
