/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportMultiSearchAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.ResultsAndErrors;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesResponse;
import org.elasticsearch.xpack.core.security.action.profile.UpdateProfileDataRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.DomainConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmDomain;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecurityIndexManager.IndexState;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.action.bulk.TransportSingleItemBulkWriteAction.toSingleItemBulkRequest;
import static org.elasticsearch.common.Strings.collectionToCommaDelimitedString;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_PROFILE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.security.authc.Authentication.isFileOrNativeRealm;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.PRIMARY_SHARDS;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.Availability.SEARCH_SHARDS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;

public class ProfileService {
    private static final Logger logger = LogManager.getLogger(ProfileService.class);
    private static final String DOC_ID_PREFIX = "profile_";
    private static final BackoffPolicy DEFAULT_BACKOFF = BackoffPolicy.exponentialBackoff();
    private static final int DIFFERENTIATOR_UPPER_LIMIT = 9;
    private static final long ACTIVATE_INTERVAL_IN_MS = TimeValue.timeValueSeconds(30).millis();

    private final Settings settings;
    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager profileIndex;
    private final Function<String, DomainConfig> domainConfigLookup;
    private final Function<RealmConfig.RealmIdentifier, Authentication.RealmRef> realmRefLookup;

    public ProfileService(Settings settings, Clock clock, Client client, SecurityIndexManager profileIndex, Realms realms) {
        this.settings = settings;
        this.clock = clock;
        this.client = client;
        this.profileIndex = profileIndex;
        this.domainConfigLookup = realms::getDomainConfig;
        this.realmRefLookup = realms::getRealmRef;
    }

    public void getProfiles(List<String> uids, Set<String> dataKeys, ActionListener<ResultsAndErrors<Profile>> listener) {
        getVersionedDocuments(uids, listener.map(resultsAndErrors -> {
            if (resultsAndErrors != null) {
                return new ResultsAndErrors<>(
                    resultsAndErrors.results().stream().map(versionedDocument -> versionedDocument.toProfile(dataKeys)).toList(),
                    resultsAndErrors.errors()
                );
            } else {
                return new ResultsAndErrors<>(
                    List.of(),
                    uids.stream()
                        .collect(
                            Collectors.toUnmodifiableMap(
                                Function.identity(),
                                uid -> new ElasticsearchException("profile index does not exist")
                            )
                        )
                );
            }
        }));
    }

    public void getProfileSubjects(Collection<String> uids, ActionListener<ResultsAndErrors<Map.Entry<String, Subject>>> listener) {
        getVersionedDocuments(uids, listener.map(resultsAndErrors -> {
            if (resultsAndErrors != null) {
                // convert the list of profile document to a list of "uid to subject" entries
                return new ResultsAndErrors<>(
                    resultsAndErrors.results()
                        .stream()
                        .map(VersionedDocument::doc)
                        .filter(ProfileDocument::enabled)
                        .map(doc -> Map.entry(doc.uid(), doc.user().toSubject()))
                        .toList(),
                    resultsAndErrors.errors()
                );
            } else {
                return new ResultsAndErrors<>(List.of(), Map.of());
            }
        }));
    }

    // TODO: with request when we take request body for profile activation
    /**
     * Create a new profile or update an existing profile for the user of the given Authentication.
     * @param authentication This is the object from which the profile will be created or updated.
     *                       It contains information about the username and relevant realms and domain.
     *                       Note that this authentication object does not belong to the authenticating user
     *                       because the associated ActivateProfileRequest provides the authentication information
     *                       in the request body while the authenticating user is the one that has privileges
     *                       to submit the request.
     */
    public void activateProfile(Authentication authentication, ActionListener<Profile> listener) {
        final Subject subject = authentication.getEffectiveSubject();
        if (Subject.Type.USER != subject.getType()) {
            listener.onFailure(
                new IllegalArgumentException(
                    "profile is supported for user only, but subject is a [" + subject.getType().name().toLowerCase(Locale.ROOT) + "]"
                )
            );
            return;
        }

        if (subject.getUser() instanceof InternalUser) {
            listener.onFailure(
                new IllegalStateException("profile should not be created for internal user [" + subject.getUser().principal() + "]")
            );
            return;
        }

        searchVersionedDocumentForSubject(subject, ActionListener.wrap(versionedDocument -> {
            if (versionedDocument == null) {
                final DomainConfig domainConfig = getDomainConfigForSubject(subject);
                if (domainConfig == null || false == domainConfig.literalUsername()) {
                    assert domainConfig == null || domainConfig.suffix() == null;
                    // The initial differentiator is 0 for new profile
                    createNewProfile(subject, ProfileDocument.computeBaseUidForSubject(subject) + "_0", listener);
                } else {
                    assert domainConfig.suffix() != null;
                    validateUsername(subject);
                    createNewProfile(subject, "u_" + subject.getUser().principal() + "_" + domainConfig.suffix(), listener);
                }
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
            {
                builder.field("user_profile");
                builder.startObject();
                {
                    if (false == request.getLabels().isEmpty()) {
                        builder.field("labels", request.getLabels());
                    }
                    if (false == request.getData().isEmpty()) {
                        builder.field("application_data", request.getData());
                    }
                }
                builder.endObject();
            }
            builder.endObject();
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }

        doUpdate(
            buildUpdateRequest(request.getUid(), builder, request.getRefreshPolicy(), request.getIfPrimaryTerm(), request.getIfSeqNo()),
            listener.map(updateResponse -> AcknowledgedResponse.TRUE)
        );
    }

    public void suggestProfile(SuggestProfilesRequest request, TaskId parentTaskId, ActionListener<SuggestProfilesResponse> listener) {
        tryFreezeAndCheckIndex(listener.map(response -> {
            assert response == null : "only null response can reach here";
            return new SuggestProfilesResponse(new SuggestProfilesResponse.ProfileHit[] {}, 0, Lucene.TOTAL_HITS_EQUAL_TO_ZERO);
        }), SEARCH_SHARDS).ifPresent(frozenProfileIndex -> {
            final SearchRequest searchRequest = buildSearchRequestForSuggest(request, parentTaskId);

            frozenProfileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_PROFILE_ORIGIN,
                    TransportSearchAction.TYPE,
                    searchRequest,
                    ActionListener.wrap(searchResponse -> {
                        final SearchHits searchHits = searchResponse.getHits();
                        final SearchHit[] hits = searchHits.getHits();
                        final SuggestProfilesResponse.ProfileHit[] profileHits;
                        if (hits.length == 0) {
                            profileHits = new SuggestProfilesResponse.ProfileHit[0];
                        } else {
                            profileHits = new SuggestProfilesResponse.ProfileHit[hits.length];
                            for (int i = 0; i < hits.length; i++) {
                                final SearchHit hit = hits[i];
                                final VersionedDocument versionedDocument = new VersionedDocument(
                                    buildProfileDocument(hit.getSourceRef()),
                                    hit.getPrimaryTerm(),
                                    hit.getSeqNo()
                                );
                                profileHits[i] = new SuggestProfilesResponse.ProfileHit(
                                    versionedDocument.toProfile(request.getDataKeys()),
                                    hit.getScore()
                                );
                            }
                        }
                        listener.onResponse(
                            new SuggestProfilesResponse(profileHits, searchResponse.getTook().millis(), searchHits.getTotalHits())
                        );
                    }, listener::onFailure)
                )
            );
        });
    }

    public void setEnabled(String uid, boolean enabled, RefreshPolicy refreshPolicy, ActionListener<AcknowledgedResponse> listener) {
        final XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject().field("user_profile", Map.of("enabled", enabled)).endObject();
        } catch (IOException e) {
            listener.onFailure(e);
            return;
        }
        doUpdate(buildUpdateRequest(uid, builder, refreshPolicy), listener.map(updateResponse -> AcknowledgedResponse.TRUE));
    }

    public void resolveProfileUidsForApiKeys(Collection<ApiKey> apiKeyInfos, ActionListener<Collection<String>> listener) {
        List<Subject> subjects = apiKeyInfos.stream().map(this::getApiKeyCreatorSubject).filter(Objects::nonNull).distinct().toList();
        searchProfilesForSubjects(subjects, ActionListener.wrap(resultsAndErrors -> {
            if (resultsAndErrors == null) {
                // profile index does not exist
                listener.onResponse(null);
            } else if (resultsAndErrors.errors().isEmpty()) {
                assert subjects.size() == resultsAndErrors.results().size();
                Map<Subject, String> profileUidLookup = resultsAndErrors.results()
                    .stream()
                    .filter(t -> Objects.nonNull(t.v2()))
                    .map(t -> new Tuple<>(t.v1(), t.v2().uid()))
                    .collect(Collectors.toUnmodifiableMap(Tuple::v1, Tuple::v2));
                listener.onResponse(apiKeyInfos.stream().map(apiKeyInfo -> {
                    Subject subject = getApiKeyCreatorSubject(apiKeyInfo);
                    return subject == null ? null : profileUidLookup.get(subject);
                }).toList());
            } else {
                final ElasticsearchStatusException exception = new ElasticsearchStatusException(
                    "failed to retrieve profile for users. please retry without fetching profile uid (with_profile_uid=false)",
                    RestStatus.INTERNAL_SERVER_ERROR
                );
                resultsAndErrors.errors().values().forEach(exception::addSuppressed);
                listener.onFailure(exception);
            }
        }, listener::onFailure));
    }

    public void searchProfilesForSubjects(List<Subject> subjects, ActionListener<SubjectSearchResultsAndErrors<Profile>> listener) {
        searchVersionedDocumentsForSubjects(subjects, ActionListener.wrap(resultsAndErrors -> {
            if (resultsAndErrors == null) {
                // profile index does not exist
                listener.onResponse(null);
                return;
            }
            listener.onResponse(new SubjectSearchResultsAndErrors<>(resultsAndErrors.results().stream().map(t -> {
                if (t.v2() != null) {
                    return new Tuple<>(t.v1(), t.v2().toProfile(Set.of()));
                } else {
                    return new Tuple<>(t.v1(), (Profile) null);
                }
            }).toList(), resultsAndErrors.errors()));
        }, listener::onFailure));
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        tryFreezeAndCheckIndex(listener.map(response -> { // index does not exist
            assert response == null : "only null response can reach here";
            return Map.of("total", 0L, "enabled", 0L, "recent", 0L);
        }), SEARCH_SHARDS).ifPresent(frozenProfileIndex -> {
            final MultiSearchRequest multiSearchRequest = client.prepareMultiSearch()
                .add(
                    client.prepareSearch(SECURITY_PROFILE_ALIAS)
                        .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("user_profile.uid")))
                        .setSize(0)
                        .setTrackTotalHits(true)
                        .request()
                )
                .add(
                    client.prepareSearch(SECURITY_PROFILE_ALIAS)
                        .setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("user_profile.enabled", true)))
                        .setSize(0)
                        .setTrackTotalHits(true)
                        .request()
                )
                .add(
                    client.prepareSearch(SECURITY_PROFILE_ALIAS)
                        .setQuery(
                            QueryBuilders.boolQuery()
                                .filter(QueryBuilders.termQuery("user_profile.enabled", true))
                                .filter(
                                    QueryBuilders.rangeQuery("user_profile.last_synchronized")
                                        .gt(Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli())
                                )
                        )
                        .setSize(0)
                        .setTrackTotalHits(true)
                        .request()
                )
                .request();

            frozenProfileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_PROFILE_ORIGIN,
                    TransportMultiSearchAction.TYPE,
                    multiSearchRequest,
                    ActionListener.wrap(multiSearchResponse -> {
                        final MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
                        assert items.length == 3;
                        final Map<String, Object> usage = new HashMap<>();
                        if (items[0].isFailure()) {
                            logger.debug("error on counting total profiles", items[0].getFailure());
                            usage.put("total", 0L);
                        } else {
                            usage.put("total", items[0].getResponse().getHits().getTotalHits().value());
                        }
                        if (items[1].isFailure()) {
                            logger.debug("error on counting enabled profiles", items[0].getFailure());
                            usage.put("enabled", 0L);
                        } else {
                            usage.put("enabled", items[1].getResponse().getHits().getTotalHits().value());
                        }
                        if (items[2].isFailure()) {
                            logger.debug("error on counting recent profiles", items[0].getFailure());
                            usage.put("recent", 0L);
                        } else {
                            usage.put("recent", items[2].getResponse().getHits().getTotalHits().value());
                        }
                        listener.onResponse(usage);
                    }, listener::onFailure)
                )
            );
        });
    }

    // package private for testing
    SearchRequest buildSearchRequestForSuggest(SuggestProfilesRequest request, TaskId parentTaskId) {
        final BoolQueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("user_profile.enabled", true));
        if (Strings.hasText(request.getName())) {
            query.must(
                QueryBuilders.multiMatchQuery(
                    request.getName(),
                    "user_profile.user.username",
                    "user_profile.user.username._2gram",
                    "user_profile.user.username._3gram",
                    "user_profile.user.full_name",
                    "user_profile.user.full_name._2gram",
                    "user_profile.user.full_name._3gram",
                    "user_profile.user.email"
                ).type(MultiMatchQueryBuilder.Type.BOOL_PREFIX).fuzziness(Fuzziness.AUTO)
            );
        }
        final SuggestProfilesRequest.Hint hint = request.getHint();
        if (hint != null) {
            final List<String> hintedUids = hint.getUids();
            if (hintedUids != null) {
                assert false == hintedUids.isEmpty() : "uids hint cannot be empty";
                query.should(QueryBuilders.termsQuery("user_profile.uid", hintedUids));
            }
            final Tuple<String, List<String>> label = hint.getSingleLabel();
            if (label != null) {
                final List<String> labelValues = label.v2();
                query.should(QueryBuilders.termsQuery("user_profile.labels." + label.v1(), labelValues));
            }
            query.minimumShouldMatch(0);
        }

        final SearchRequest searchRequest = client.prepareSearch(SECURITY_PROFILE_ALIAS)
            .setQuery(query)
            .setSize(request.getSize())
            .addSort("_score", SortOrder.DESC)
            .addSort("user_profile.last_synchronized", SortOrder.DESC)
            .request();
        searchRequest.setParentTask(parentTaskId);
        return searchRequest;
    }

    private void getVersionedDocument(String uid, ActionListener<VersionedDocument> listener) {
        tryFreezeAndCheckIndex(listener, PRIMARY_SHARDS).ifPresent(frozenProfileIndex -> {
            final GetRequest getRequest = new GetRequest(SECURITY_PROFILE_ALIAS, uidToDocId(uid));
            frozenProfileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_PROFILE_ORIGIN,
                    TransportGetAction.TYPE,
                    getRequest,
                    ActionListener.wrap(response -> {
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
                    }, listener::onFailure)
                )
            );
        });
    }

    private void getVersionedDocuments(Collection<String> uids, ActionListener<ResultsAndErrors<VersionedDocument>> listener) {
        if (uids.isEmpty()) {
            listener.onResponse(ResultsAndErrors.empty());
            return;
        }
        tryFreezeAndCheckIndex(listener, PRIMARY_SHARDS).ifPresent(frozenProfileIndex -> {
            frozenProfileIndex.checkIndexVersionThenExecute(
                listener::onFailure,
                () -> new OriginSettingClient(client, SECURITY_PROFILE_ORIGIN).prepareMultiGet()
                    .addIds(frozenProfileIndex.aliasName(), uids.stream().map(ProfileService::uidToDocId).toArray(String[]::new))
                    .execute(ActionListener.wrap(multiGetResponse -> {
                        List<VersionedDocument> retrievedDocs = new ArrayList<>(multiGetResponse.getResponses().length);
                        // ordered for tests
                        final Map<String, Exception> errors = new TreeMap<>();
                        for (MultiGetItemResponse itemResponse : multiGetResponse.getResponses()) {
                            final String profileUid = docIdToUid(itemResponse.getId());
                            if (itemResponse.isFailed()) {
                                logger.debug("Failed to retrieve profile [{}]", profileUid);
                                errors.put(profileUid, itemResponse.getFailure().getFailure());
                            } else if (itemResponse.getResponse() != null) {
                                if (itemResponse.getResponse().isExists()) {
                                    retrievedDocs.add(
                                        new VersionedDocument(
                                            buildProfileDocument(itemResponse.getResponse().getSourceAsBytesRef()),
                                            itemResponse.getResponse().getPrimaryTerm(),
                                            itemResponse.getResponse().getSeqNo()
                                        )
                                    );
                                } else {
                                    logger.debug("Profile [{}] not found", profileUid);
                                    errors.put(profileUid, new ResourceNotFoundException("profile document not found"));
                                }
                            } else {
                                assert false
                                    : "Inconsistent mget item response [" + itemResponse.getIndex() + "] [" + itemResponse.getId() + "]";
                                logger.error("Inconsistent mget item response [{}] [{}]", itemResponse.getIndex(), itemResponse.getId());
                            }
                        }
                        listener.onResponse(new ResultsAndErrors<>(retrievedDocs, errors));
                    }, listener::onFailure))
            );
        });
    }

    // Package private for testing
    void searchVersionedDocumentForSubject(Subject subject, ActionListener<VersionedDocument> listener) {
        searchVersionedDocumentsForSubjects(List.of(subject), ActionListener.wrap(resultsAndErrors -> {
            if (resultsAndErrors == null) {
                // profile index does not exist
                listener.onResponse(null);
                return;
            }

            assert resultsAndErrors.results().size() + resultsAndErrors.errors().size() == 1
                : "a single subject must have either a single result or error";
            if (resultsAndErrors.results().size() == 1) {
                listener.onResponse(resultsAndErrors.results().iterator().next().v2());
            } else if (resultsAndErrors.errors().size() == 1) {
                final Exception exception = resultsAndErrors.errors().values().iterator().next();
                logger.error(exception.getMessage());
                listener.onFailure(exception);
            } else {
                assert false : "a single subject must have either a single result or error";
                listener.onFailure(new ElasticsearchException("a single subject must have either a single result or error"));
            }

        }, listener::onFailure));
    }

    private void searchVersionedDocumentsForSubjects(
        List<Subject> subjects,
        ActionListener<SubjectSearchResultsAndErrors<VersionedDocument>> listener
    ) {
        if (subjects.isEmpty()) {
            listener.onResponse(new SubjectSearchResultsAndErrors<>(List.of(), Map.of()));
            return;
        }
        tryFreezeAndCheckIndex(listener, SEARCH_SHARDS).ifPresent(frozenProfileIndex -> {
            frozenProfileIndex.checkIndexVersionThenExecute(listener::onFailure, () -> {
                final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
                subjects.forEach(subject -> multiSearchRequest.add(buildSearchRequestForSubject(subject)));
                executeAsyncWithOrigin(
                    client,
                    SECURITY_PROFILE_ORIGIN,
                    TransportMultiSearchAction.TYPE,
                    multiSearchRequest,
                    ActionListener.wrap(
                        multiSearchResponse -> listener.onResponse(convertSubjectMultiSearchResponse(multiSearchResponse, subjects)),
                        listener::onFailure
                    )
                );
            });
        });
    }

    private static SubjectSearchResultsAndErrors<VersionedDocument> convertSubjectMultiSearchResponse(
        MultiSearchResponse multiSearchResponse,
        List<Subject> subjects
    ) throws IOException {
        final MultiSearchResponse.Item[] items = multiSearchResponse.getResponses();
        assert items.length == subjects.size() : "size of responses does not match size of subjects";
        final List<Tuple<Subject, VersionedDocument>> versionedDocs = new ArrayList<>(items.length);
        final Map<Subject, Exception> errors = new HashMap<>();
        for (int i = 0; i < items.length; i++) {
            final MultiSearchResponse.Item item = items[i];
            final Subject subject = subjects.get(i);
            if (item.isFailure()) {
                errors.put(subject, item.getFailure());
            } else {
                final SearchHits searchHits = item.getResponse().getHits();
                final SearchHit[] hits = searchHits.getHits();
                if (hits.length < 1) {
                    logger.debug(
                        "profile does not exist for username [{}] and realm name [{}]",
                        subject.getUser().principal(),
                        subject.getRealm().getName()
                    );
                    versionedDocs.add(new Tuple<>(subject, null));
                } else if (hits.length == 1) {
                    final SearchHit hit = hits[0];
                    final ProfileDocument profileDocument = buildProfileDocument(hit.getSourceRef());
                    if (subject.canAccessResourcesOf(profileDocument.user().toSubject())) {
                        versionedDocs.add(
                            new Tuple<>(subject, new VersionedDocument(profileDocument, hit.getPrimaryTerm(), hit.getSeqNo()))
                        );
                    } else {
                        assert false : "this should not happen";
                        errors.put(
                            subject,
                            new ElasticsearchException(
                                format(
                                    "profile [%s] matches search criteria but is not accessible to "
                                        + "the current subject with username [%s] and realm name [%s]",
                                    profileDocument.uid(),
                                    subject.getUser().principal(),
                                    subject.getRealm().getName()
                                )
                            )
                        );
                    }
                } else {
                    errors.put(
                        subject,
                        new ElasticsearchException(
                            format(
                                "multiple [%s] profiles [%s] found for user [%s] from realm [%s]%s",
                                hits.length,
                                Arrays.stream(hits)
                                    .map(SearchHit::getId)
                                    .map(ProfileService::docIdToUid)
                                    .sorted()
                                    .collect(Collectors.joining(",")),
                                subject.getUser().principal(),
                                subject.getRealm().getName(),
                                subject.getRealm().getDomain() == null
                                    ? ""
                                    : (" under domain [" + subject.getRealm().getDomain().name() + "]")
                            )
                        )
                    );
                }
            }
        }
        return new SubjectSearchResultsAndErrors<>(versionedDocs, errors);
    }

    private SearchRequest buildSearchRequestForSubject(Subject subject) {
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("user_profile.user.username.keyword", subject.getUser().principal()));
        if (subject.getRealm().getDomain() == null) {
            boolQuery.filter(QueryBuilders.termQuery("user_profile.user.realm.type", subject.getRealm().getType()));
            if (false == isFileOrNativeRealm(subject.getRealm().getType())) {
                boolQuery.filter(QueryBuilders.termQuery("user_profile.user.realm.name", subject.getRealm().getName()));
            }
        } else {
            logger.debug(
                () -> format(
                    "searching existing profile document for user [%s] from any of the realms [%s] under domain [%s]",
                    subject.getUser().principal(),
                    collectionToCommaDelimitedString(subject.getRealm().getDomain().realms()),
                    subject.getRealm().getDomain().name()
                )
            );
            subject.getRealm().getDomain().realms().forEach(realmIdentifier -> {
                final BoolQueryBuilder perRealmQuery = QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("user_profile.user.realm.type", realmIdentifier.getType()));
                if (false == isFileOrNativeRealm(realmIdentifier.getType())) {
                    perRealmQuery.filter(QueryBuilders.termQuery("user_profile.user.realm.name", realmIdentifier.getName()));
                }
                boolQuery.should(perRealmQuery);
            });
            boolQuery.minimumShouldMatch(1);
        }
        return client.prepareSearch(SECURITY_PROFILE_ALIAS).setQuery(boolQuery).seqNoAndPrimaryTerm(true).request();
    }

    private static final Pattern VALID_LITERAL_USERNAME = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9-]{0,255}$");

    private static final String INVALID_USERNAME_MESSAGE = "Security domain [%s] is configured to use literal username. "
        + "As a result, creating new user profile requires the username to be at least 1 and no more than 256 characters. "
        + "The username must begin with an alphanumeric character (a-z, A-Z, 0-9) and followed by any alphanumeric "
        + "or dash (-) characters.";

    private void validateUsername(Subject subject) {
        final RealmDomain realmDomain = subject.getRealm().getDomain();
        assert realmDomain != null;
        assert domainConfigLookup.apply(realmDomain.name()) != null;
        assert domainConfigLookup.apply(realmDomain.name()).literalUsername();

        final String username = subject.getUser().principal();
        assert username != null;

        if (false == VALID_LITERAL_USERNAME.matcher(username).matches()) {
            throw new ElasticsearchException(String.format(Locale.ROOT, INVALID_USERNAME_MESSAGE, realmDomain.name()));
        }
    }

    // Package private for testing
    void createNewProfile(Subject subject, String uid, ActionListener<Profile> listener) throws IOException {
        // When the code reaches here, we are sure no existing profile matches the subject's username and realm info
        // We go ahead to create the new profile document. If there is another concurrent creation request, it should
        // attempt to create a doc with the same ID and cause version conflict which is handled.
        final ProfileDocument profileDocument = ProfileDocument.fromSubjectWithUid(subject, uid);
        final String docId = uidToDocId(profileDocument.uid());
        final BulkRequest bulkRequest = toSingleItemBulkRequest(
            client.prepareIndex(SECURITY_PROFILE_ALIAS)
                .setId(docId)
                .setSource(wrapProfileDocument(profileDocument))
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
                .request()
        );
        profileIndex.forCurrentProject()
            .prepareIndexIfNeededThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_PROFILE_ORIGIN,
                    TransportBulkAction.TYPE,
                    bulkRequest,
                    TransportBulkAction.<IndexResponse>unwrappingSingleItemBulkResponse(ActionListener.wrap(indexResponse -> {
                        assert docId.equals(indexResponse.getId());
                        final VersionedDocument versionedDocument = new VersionedDocument(
                            profileDocument,
                            indexResponse.getPrimaryTerm(),
                            indexResponse.getSeqNo()
                        );
                        listener.onResponse(versionedDocument.toProfile(Set.of()));
                    }, e -> {
                        if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                            // Document already exists with the specified ID, get the document with the ID
                            // and check whether it is the right profile for the subject
                            getOrCreateProfileWithBackoff(subject, profileDocument, DEFAULT_BACKOFF.iterator(), listener);
                        } else {
                            listener.onFailure(e);
                        }
                    }))
                )
            );
    }

    // Package private for test
    void getOrCreateProfileWithBackoff(
        Subject subject,
        ProfileDocument profileDocument,
        Iterator<TimeValue> backoff,
        ActionListener<Profile> listener
    ) {
        getVersionedDocument(profileDocument.uid(), ActionListener.wrap(versionedDocument -> {
            if (versionedDocument == null) {
                // Document not found. This can happen if the GET request hits a replica that is still processing the document
                if (backoff.hasNext()) {
                    final TimeValue backoffTimeValue = backoff.next();
                    logger.debug("retrying get profile document [{}] after [{}] backoff", profileDocument.uid(), backoffTimeValue);
                    client.threadPool()
                        .schedule(
                            () -> getOrCreateProfileWithBackoff(subject, profileDocument, backoff, listener),
                            backoffTimeValue,
                            client.threadPool().generic()
                        );
                } else {
                    // Retry has depleted. This can only happen when the document or the profile index itself gets deleted
                    // in between requests.
                    listener.onFailure(
                        new ElasticsearchException("failed to retrieving profile [{}] after all retries", profileDocument.uid())
                    );
                }
                return;
            }
            // Ownership check between the subject and the profile document
            if (subject.canAccessResourcesOf(versionedDocument.doc.user().toSubject())) {
                // The profile document can be accessed by the subject. It must have just got created by another thread, i.e. racing.
                // Still need to update it with current auth info before return.
                logger.debug(
                    "found existing profile document [{}] accessible to the current subject with username [{}] and realm name [{}]",
                    versionedDocument.doc.uid(),
                    subject.getUser().principal(),
                    subject.getRealm().getName()
                );
                updateProfileForActivate(subject, versionedDocument, listener);
            } else {
                // The profile document is NOT a match, this means either:
                // 1. Genuine hash collision
                // 2. A different user has the same username
                // 3. Profile document was manually updated
                // So we attempt to differentiate from the existing profile document by increase the differentiator number by 1.
                maybeIncrementDifferentiatorAndCreateNewProfile(subject, profileDocument, listener);
            }
        }, listener::onFailure));
    }

    // Package private for tests
    void maybeIncrementDifferentiatorAndCreateNewProfile(Subject subject, ProfileDocument profileDocument, ActionListener<Profile> listener)
        throws IOException {
        final String uid = profileDocument.uid();
        final int index = uid.lastIndexOf('_');
        if (index == -1) {
            listener.onFailure(new ElasticsearchException("profile uid [{}] does not contain any underscore character", uid));
            return;
        }
        final String baseUid = uid.substring(0, index);
        final String differentiatorString = uid.substring(index + 1);
        if (differentiatorString.isBlank()) {
            listener.onFailure(new ElasticsearchException("profile uid [{}] does not contain a differentiator", uid));
            return;
        }

        final DomainConfig domainConfig = getDomainConfigForSubject(subject);
        // The user is from a domain that is configured to have a fixed suffix and should not auto-increment for clashing UID
        if (domainConfig != null && domainConfig.suffix() != null) {
            assert differentiatorString.equals(domainConfig.suffix());
            listener.onFailure(
                new ElasticsearchException(
                    "cannot create new profile for ["
                        + subject.getUser().principal()
                        + "]."
                        + " A profile with uid ["
                        + profileDocument.uid()
                        + "] already exists and suffix setting of domain ["
                        + domainConfig.name()
                        + "] does not support auto-increment."
                )
            );
            return;
        }

        final int differentiator;
        try {
            differentiator = Integer.parseInt(differentiatorString);
        } catch (NumberFormatException e) {
            listener.onFailure(new ElasticsearchException("profile uid [{}] differentiator is not a number", e, uid));
            return;
        }
        // Prevent infinite recursion. It is practically impossible to get this many clashes
        if (differentiator >= DIFFERENTIATOR_UPPER_LIMIT) {
            listener.onFailure(
                new ElasticsearchException("profile differentiator value is too high for base Uid [{}]", uid.substring(0, index))
            );
            return;
        }
        // New uid by increment the differentiator by 1
        final String newUid = baseUid + "_" + (differentiator + 1);
        createNewProfile(subject, newUid, listener);
    }

    private DomainConfig getDomainConfigForSubject(Subject subject) {
        final RealmDomain realmDomain = subject.getRealm().getDomain();
        if (realmDomain != null) {
            final DomainConfig domainConfig = domainConfigLookup.apply(realmDomain.name());
            if (domainConfig == null) {
                throw new ElasticsearchException(
                    "subject realm is under a domain [" + realmDomain.name() + "], but no associated domain config is found"
                );
            }
            return domainConfig;
        } else {
            return null;
        }
    }

    private Subject getApiKeyCreatorSubject(ApiKey apiKeyInfo) {
        if (apiKeyInfo.getUsername() == null) {
            logger.debug("encountered api key with id [{}] of the \"null\" username", apiKeyInfo.getId());
            return null;
        }
        RealmConfig.RealmIdentifier realmIdentifier = apiKeyInfo.getRealmIdentifier();
        if (realmIdentifier == null) {
            logger.debug(
                "encountered api key with id [{}] of the username [{}] that has a \"null\" realm type or realm name",
                apiKeyInfo.getId(),
                apiKeyInfo.getUsername()
            );
            return null;
        }
        Authentication.RealmRef realmRef = realmRefLookup.apply(realmIdentifier);
        if (realmRef == null) {
            logger.debug(
                "encountered api key with id [{}] of the username [{}] from realm [{}], "
                    + "where that realm is not currently configured on the local node",
                apiKeyInfo.getId(),
                apiKeyInfo.getUsername(),
                realmIdentifier
            );
            return null;
        }
        return new Subject(new User(apiKeyInfo.getUsername(), Strings.EMPTY_ARRAY), realmRef);
    }

    // package private for testing
    void updateProfileForActivate(Subject subject, VersionedDocument currentVersionedDocumentBySearch, ActionListener<Profile> listener)
        throws IOException {
        final ProfileDocument newProfileDocument = updateWithSubject(currentVersionedDocumentBySearch.doc, subject);

        if (shouldSkipUpdateForActivate(currentVersionedDocumentBySearch.doc, newProfileDocument)) {
            logger.debug(
                "skip user profile activate update because last_synchronized [{}] is within grace period",
                currentVersionedDocumentBySearch.doc.lastSynchronized()
            );
            listener.onResponse(currentVersionedDocumentBySearch.toProfile(Set.of()));
            return;
        }

        doUpdate(
            buildUpdateRequest(
                newProfileDocument.uid(),
                wrapProfileDocumentWithoutApplicationData(newProfileDocument),
                RefreshPolicy.WAIT_UNTIL
            ),
            ActionListener.wrap(updateResponse -> {
                listener.onResponse(
                    new VersionedDocument(newProfileDocument, updateResponse.getPrimaryTerm(), updateResponse.getSeqNo()).toProfile(
                        Set.of()
                    )
                );
            }, updateException -> {
                // The document may have been updated concurrently by another thread. Get it and check whether the updated content
                // already has what is required by this thread. If so, simply return the updated profile.
                if (ExceptionsHelper.unwrapCause(updateException) instanceof VersionConflictEngineException) {
                    getVersionedDocument(currentVersionedDocumentBySearch.doc.uid(), ActionListener.wrap(versionedDocumentByGet -> {
                        if (shouldSkipUpdateForActivate(versionedDocumentByGet.doc, newProfileDocument)) {
                            logger.debug(
                                "suppress version conflict for activate update because last_synchronized [{}] is within grace period",
                                versionedDocumentByGet.doc.lastSynchronized()
                            );
                            listener.onResponse(versionedDocumentByGet.toProfile(Set.of()));
                        } else {
                            listener.onFailure(updateException);
                        }
                    }, getException -> {
                        getException.addSuppressed(updateException);
                        listener.onFailure(getException);
                    }));
                } else {
                    listener.onFailure(updateException);
                }
            })
        );
    }

    // If the profile content does not change and it is recently updated within last 30 seconds, do not update it again
    // to avoid potential excessive version conflicts
    boolean shouldSkipUpdateForActivate(ProfileDocument currentProfileDocument, ProfileDocument newProfileDocument) {
        assert newProfileDocument.enabled() : "new profile document must be enabled";
        if (newProfileDocument.user().equals(currentProfileDocument.user())
            && newProfileDocument.enabled() == currentProfileDocument.enabled()
            && newProfileDocument.lastSynchronized() - currentProfileDocument.lastSynchronized() < ACTIVATE_INTERVAL_IN_MS) {
            return true;
        }
        return false;
    }

    private UpdateRequest buildUpdateRequest(String uid, XContentBuilder builder, RefreshPolicy refreshPolicy) {
        return buildUpdateRequest(uid, builder, refreshPolicy, -1, -1);
    }

    private UpdateRequest buildUpdateRequest(
        String uid,
        XContentBuilder builder,
        RefreshPolicy refreshPolicy,
        long ifPrimaryTerm,
        long ifSeqNo
    ) {
        final String docId = uidToDocId(uid);
        final UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate(SECURITY_PROFILE_ALIAS, docId)
            .setDoc(builder)
            .setRefreshPolicy(refreshPolicy);

        if (ifPrimaryTerm >= 0) {
            updateRequestBuilder.setIfPrimaryTerm(ifPrimaryTerm);
        }
        if (ifSeqNo >= 0) {
            updateRequestBuilder.setIfSeqNo(ifSeqNo);
        }
        return updateRequestBuilder.request();
    }

    // Package private for testing
    void doUpdate(UpdateRequest updateRequest, ActionListener<UpdateResponse> listener) {
        profileIndex.forCurrentProject()
            .prepareIndexIfNeededThenExecute(
                listener::onFailure,
                () -> executeAsyncWithOrigin(
                    client,
                    SECURITY_PROFILE_ORIGIN,
                    TransportUpdateAction.TYPE,
                    updateRequest,
                    ActionListener.wrap(updateResponse -> {
                        assert updateResponse.getResult() == DocWriteResponse.Result.UPDATED
                            || updateResponse.getResult() == DocWriteResponse.Result.NOOP;
                        listener.onResponse(updateResponse);
                    }, listener::onFailure)
                )
            );
    }

    private static String uidToDocId(String uid) {
        return DOC_ID_PREFIX + uid;
    }

    private static String docIdToUid(String docId) {
        if (docId == null || false == docId.startsWith(DOC_ID_PREFIX)) {
            throw new IllegalStateException("profile document ID [" + docId + "] has unexpected value");
        }
        return docId.substring(DOC_ID_PREFIX.length());
    }

    static ProfileDocument buildProfileDocument(BytesReference source) throws IOException {
        if (source == null) {
            throw new IllegalStateException("profile document did not have source but source should have been fetched");
        }
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, XContentType.JSON)) {
            return ProfileDocument.fromXContent(parser);
        }
    }

    private static XContentBuilder wrapProfileDocument(ProfileDocument profileDocument) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("user_profile", profileDocument);
        builder.endObject();
        return builder;
    }

    private static XContentBuilder wrapProfileDocumentWithoutApplicationData(ProfileDocument profileDocument) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field(
            "user_profile",
            profileDocument,
            // NOT including the labels and data in the update request so they will not be changed
            new ToXContent.MapParams(Map.of("include_labels", Boolean.FALSE.toString(), "include_data", Boolean.FALSE.toString()))
        );
        builder.endObject();
        return builder;
    }

    /**
     * Freeze the profile index check its availability and return it if everything is ok.
     * Otherwise it calls the listener with null and returns an empty Optional.
     */
    private <T> Optional<IndexState> tryFreezeAndCheckIndex(ActionListener<T> listener, SecurityIndexManager.Availability availability) {
        final IndexState projectSecurityIndex = profileIndex.forCurrentProject();
        if (false == projectSecurityIndex.indexExists()) {
            logger.debug("profile index does not exist");
            listener.onResponse(null);
            return Optional.empty();
        } else if (false == projectSecurityIndex.isAvailable(availability)) {
            listener.onFailure(projectSecurityIndex.getUnavailableReason(availability));
            return Optional.empty();
        }
        return Optional.of(projectSecurityIndex);
    }

    private static ProfileDocument updateWithSubject(ProfileDocument doc, Subject subject) {
        final User subjectUser = subject.getUser();
        return new ProfileDocument(
            doc.uid(),
            true,
            Instant.now().toEpochMilli(),
            new ProfileDocument.ProfileDocumentUser(
                subjectUser.principal(),
                Arrays.asList(subjectUser.roles()),
                subject.getRealm(),
                // Replace with incoming information even when they are null
                subjectUser.email(),
                subjectUser.fullName()
            ),
            doc.labels(),
            doc.applicationData()
        );
    }

    // Package private for testing
    record VersionedDocument(ProfileDocument doc, long primaryTerm, long seqNo) {

        /**
         * Convert the index document to the user-facing Profile by filtering through the application data
         */
        Profile toProfile(Set<String> dataKeys) {
            assert dataKeys != null : "data keys must not be null";
            final Map<String, Object> applicationData;
            if (dataKeys.isEmpty()) {
                applicationData = Map.of();
            } else {
                applicationData = XContentHelper.convertToMap(doc.applicationData(), false, XContentType.JSON, dataKeys, null).v2();
            }

            return new Profile(
                doc.uid(),
                doc.enabled(),
                doc.lastSynchronized(),
                doc.user().toProfileUser(),
                doc.labels(),
                applicationData,
                new Profile.VersionControl(primaryTerm, seqNo)
            );
        }

    }

    public record SubjectSearchResultsAndErrors<T>(List<Tuple<Subject, T>> results, Map<Subject, Exception> errors) {}
}
