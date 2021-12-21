/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationContext;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;

public class ProfileService {
    private static final Logger logger = LogManager.getLogger(ProfileService.class);
    private static final String DOC_ID_PREFIX = "profile_";

    private final Settings settings;
    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager profileIndex;
    private final ThreadPool threadPool;

    public ProfileService(Settings settings, Clock clock, Client client, SecurityIndexManager profileIndex, ThreadPool threadPool) {
        this.settings = settings;
        this.clock = clock;
        this.client = client;
        this.profileIndex = profileIndex;
        this.threadPool = threadPool;
    }

    public void getProfile(String uid, @Nullable Set<String> dataKeys, ActionListener<Profile> listener) {
        getVersionedDocument(uid, ActionListener.wrap(versionedDocument -> {
            // TODO: replace null with actual domain lookup
            listener.onResponse(versionedDocument.toProfile(null, dataKeys));
        }, listener::onFailure));
    }

    // TODO: with request
    public void activateProfile(Authentication authentication, ActionListener<Profile> listener) {
        final Subject subject = AuthenticationContext.fromAuthentication(authentication).getEffectiveSubject();
        if (Subject.Type.USER != subject.getType()) {
            listener.onFailure(
                new IllegalArgumentException(
                    "profile is supported for user only, but subject is a [" + subject.getType().name().toLowerCase(Locale.ROOT) + "]"
                )
            );
            return;
        }

        if (User.isInternal(subject.getUser())) {
            listener.onFailure(
                new IllegalStateException("profile should not be created for internal user [" + subject.getUser().principal() + "]")
            );
            return;
        }

        getVersionedDocument(authentication, ActionListener.wrap(versionedDocument -> {
            if (versionedDocument == null) {
                createProfile(subject, listener);
            } else {
                updateProfileForActivate(subject, versionedDocument, listener);

            }
        }, listener::onFailure));
    }

    public void updateProfileData(UpdateProfileDataRequest request, ActionListener<AcknowledgedResponse> listener) {
        final XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject();
            if (false == request.getAccess().isEmpty()) {
                builder.field("access", request.getAccess());
            }
            if (false == request.getData().isEmpty()) {
                builder.field("application_data", request.getData());
            }
            builder.endObject();
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        final String docId = uidToDocId(request.getUid());
        final UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(SECURITY_PROFILE_ALIAS, docId)
            .setDoc(builder)
            .setRefreshPolicy(request.getRefreshPolicy());

        if (request.getIfPrimaryTerm() >= 0) {
            updateRequestBuilder.setIfPrimaryTerm(request.getIfPrimaryTerm());
        }
        if (request.getIfSeqNo() >= 0) {
            updateRequestBuilder.setIfSeqNo(request.getIfSeqNo());
        }

        profileIndex.prepareIndexIfNeededThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                UpdateAction.INSTANCE,
                updateRequestBuilder.request(),
                ActionListener.wrap(updateResponse -> {
                    assert updateResponse.getResult() == DocWriteResponse.Result.UPDATED
                        || updateResponse.getResult() == DocWriteResponse.Result.NOOP;
                    listener.onResponse(AcknowledgedResponse.TRUE);
                }, listener::onFailure)
            )
        );

    }

    private void getVersionedDocument(String uid, ActionListener<VersionedDocument> listener) {
        if (maybeHandleIndexStatusIssue(listener)) {
            return;
        }

        final GetRequest getRequest = new GetRequest(SECURITY_PROFILE_ALIAS, uidToDocId(uid));
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            profileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(client, SECURITY_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(response -> {
                    if (false == response.isExists()) {
                        logger.debug("profile with uid [{}] does not exist", uid);
                        listener.onResponse(null);
                        return;
                    }
                    listener.onResponse(
                        new VersionedDocument(
                            buildProfileDocument(response.getSourceAsBytesRef()),
                            response.getPrimaryTerm(),
                            response.getSeqNo()
                        )
                    );
                }, listener::onFailure))
            );
        }
    }

    // Package private for testing
    void getVersionedDocument(Authentication authentication, ActionListener<VersionedDocument> listener) {
        if (maybeHandleIndexStatusIssue(listener)) {
            return;
        }

        final SearchRequest searchRequest = client.prepareSearch(SECURITY_PROFILE_ALIAS)
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("user.username", authentication.getUser().principal()))
                    // TODO: this will be replaced by domain lookup and reverse lookup
                    .must(QueryBuilders.termQuery("user.realm.name", authentication.getSourceRealm().getName()))
            )
            .request();
        try (ThreadContext.StoredContext ignore = client.threadPool().getThreadContext().stashWithOrigin(SECURITY_ORIGIN)) {
            profileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_ORIGIN,
                    SearchAction.INSTANCE,
                    searchRequest,
                    ActionListener.wrap(searchResponse -> {
                        final SearchHits searchHits = searchResponse.getHits();
                        final SearchHit[] hits = searchHits.getHits();
                        if (hits.length < 1) {
                            logger.debug(
                                "profile does not exist for username [{}] and realm name [{}]",
                                authentication.getUser().principal(),
                                authentication.getSourceRealm().getName()
                            );
                            listener.onResponse(null);
                        } else if (hits.length == 1) {
                            final SearchHit hit = hits[0];
                            listener.onResponse(
                                new VersionedDocument(buildProfileDocument(hit.getSourceRef()), hit.getPrimaryTerm(), hit.getSeqNo())
                            );
                        } else {
                            final ParameterizedMessage errorMessage = new ParameterizedMessage(
                                "multiple [{}] profiles [{}] found for user [{}]",
                                hits.length,
                                Arrays.stream(hits).map(SearchHit::getId).map(this::docIdToUid).sorted().collect(Collectors.joining(",")),
                                // TODO: include domain information
                                authentication.getUser().principal()
                            );
                            logger.error(errorMessage);
                            listener.onFailure(new IllegalStateException(errorMessage.getFormattedMessage()));
                        }
                    }, listener::onFailure)
                )
            );
        }
    }

    private void createProfile(Subject subject, ActionListener<Profile> listener) throws IOException {
        final ProfileDocument profileDocument = ProfileDocument.fromSubject(subject);
        final String docId = uidToDocId(profileDocument.uid());
        final BulkRequest bulkRequest = toSingleItemBulkRequest(
            client.prepareIndex(SECURITY_PROFILE_ALIAS)
                .setSource(profileDocument.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
                .setId(docId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .request()
        );
        profileIndex.prepareIndexIfNeededThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                BulkAction.INSTANCE,
                bulkRequest,
                TransportSingleItemBulkWriteAction.<IndexResponse>wrapBulkResponse(ActionListener.wrap(indexResponse -> {
                    assert docId.equals(indexResponse.getId());
                    // TODO: replace realm doain with domain
                    listener.onResponse(
                        new VersionedDocument(profileDocument, indexResponse.getPrimaryTerm(), indexResponse.getSeqNo()).toProfile(null)
                    );
                }, listener::onFailure))
            )
        );
    }

    private void updateProfileForActivate(Subject subject, VersionedDocument versionedDocument, ActionListener<Profile> listener)
        throws IOException {
        final ProfileDocument profileDocument = versionedDocument.doc.updateWithSubjectAndStripApplicationData(subject);
        final String docId = uidToDocId(profileDocument.uid());
        final UpdateRequest updateRequest = client.prepareUpdate(SECURITY_PROFILE_ALIAS, docId)
            .setDoc(profileDocument.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS))
            .setIfPrimaryTerm(versionedDocument.primaryTerm)
            .setIfSeqNo(versionedDocument.seqNo)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
            .request();
        profileIndex.prepareIndexIfNeededThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client,
                SECURITY_ORIGIN,
                UpdateAction.INSTANCE,
                updateRequest,
                ActionListener.wrap(updateResponse -> {
                    assert updateResponse.getResult() == DocWriteResponse.Result.UPDATED
                        || updateResponse.getResult() == DocWriteResponse.Result.NOOP;
                    listener.onResponse(
                        new VersionedDocument(profileDocument, updateResponse.getPrimaryTerm(), updateResponse.getSeqNo()).toProfile(null)
                    );
                }, listener::onFailure)
            )
        );
    }

    private String uidToDocId(String uid) {
        return DOC_ID_PREFIX + uid;
    }

    private String docIdToUid(String docId) {
        if (docId == null || false == docId.startsWith(DOC_ID_PREFIX)) {
            throw new IllegalStateException("profile document ID [" + docId + "] has unexpected value");
        }
        return docId.substring(DOC_ID_PREFIX.length());
    }

    ProfileDocument buildProfileDocument(BytesReference source) throws IOException {
        if (source == null) {
            throw new IllegalStateException("profile document did not have source but source should have been fetched");
        }
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, XContentType.JSON)) {
            return ProfileDocument.fromXContent(parser);
        }
    }

    private <T> boolean maybeHandleIndexStatusIssue(ActionListener<T> listener) {
        final SecurityIndexManager frozenProfileIndex = profileIndex.freeze();
        if (false == frozenProfileIndex.indexExists()) {
            logger.debug("profile index does not exist");
            listener.onResponse(null);
            return true;
        } else if (false == frozenProfileIndex.isAvailable()) {
            listener.onFailure(frozenProfileIndex.getUnavailableReason());
            return true;
        }
        return false;
    }

    // Package private for testing
    record VersionedDocument(ProfileDocument doc, long primaryTerm, long seqNo) {

        Profile toProfile(@Nullable String realmDomain) {
            return toProfile(realmDomain, Set.of());
        }

        Profile toProfile(@Nullable String realmDomain, @Nullable Set<String> dataKeys) {
            final Map<String, Object> applicationData;
            if (dataKeys != null && dataKeys.isEmpty()) {
                applicationData = Map.of();
            } else {
                applicationData = XContentHelper.convertToMap(doc.applicationData(), false, XContentType.JSON, dataKeys, null).v2();
            }

            return new Profile(
                doc.uid(),
                doc.enabled(),
                doc.lastSynchronized(),
                doc.user().toProfileUser(realmDomain),
                doc.access().toProfileAccess(),
                applicationData,
                new Profile.VersionControl(primaryTerm, seqNo)
            );
        }
    }
}
