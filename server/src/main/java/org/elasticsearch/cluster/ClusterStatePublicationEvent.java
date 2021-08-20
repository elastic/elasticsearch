/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

/**
 * Represents a cluster state update computed by the {@link org.elasticsearch.cluster.service.MasterService} for publication to the cluster.
 * If publication is successful then this creates a {@link ClusterChangedEvent} which is applied on every node.
 */
public class ClusterStatePublicationEvent {

    private static final long NOT_SET = -1L;

    private final String summary;
    private final ClusterState oldState;
    private final ClusterState newState;
    private final long computationTimeMillis;
    private final long publicationStartTimeMillis;
    private volatile long publicationContextConstructionElapsedMillis = NOT_SET;
    private volatile long publicationCommitElapsedMillis = NOT_SET;
    private volatile long publicationCompletionElapsedMillis = NOT_SET;
    private volatile long masterApplyElapsedMillis = NOT_SET;

    public ClusterStatePublicationEvent(
        String summary,
        ClusterState oldState,
        ClusterState newState,
        long computationTimeMillis,
        long publicationStartTimeMillis
    ) {
        this.summary = summary;
        this.oldState = oldState;
        this.newState = newState;
        this.computationTimeMillis = computationTimeMillis;
        this.publicationStartTimeMillis = publicationStartTimeMillis;
    }

    public String getSummary() {
        return summary;
    }

    public ClusterState getOldState() {
        return oldState;
    }

    public ClusterState getNewState() {
        return newState;
    }

    public long getComputationTimeMillis() {
        return computationTimeMillis;
    }

    public long getPublicationStartTimeMillis() {
        return publicationStartTimeMillis;
    }

    public void setPublicationContextConstructionElapsedMillis(long millis) {
        assert millis >= 0;
        assert publicationContextConstructionElapsedMillis == NOT_SET;
        publicationContextConstructionElapsedMillis = millis;
    }

    public void setPublicationCommitElapsedMillis(long millis) {
        assert millis >= 0;
        assert publicationCommitElapsedMillis == NOT_SET;
        publicationCommitElapsedMillis = millis;
    }

    public void setPublicationCompletionElapsedMillis(long millis) {
        assert millis >= 0;
        assert publicationCompletionElapsedMillis == NOT_SET;
        publicationCompletionElapsedMillis = millis;
    }

    public void setMasterApplyElapsedMillis(long millis) {
        assert millis >= 0;
        assert masterApplyElapsedMillis == NOT_SET;
        masterApplyElapsedMillis = millis;
    }

    /**
     * @return how long in milliseconds it took to construct the publication context, which includes computing a cluster state diff and
     *         serializing the cluster states for future transmission.
     */
    public long getPublicationContextConstructionElapsedMillis() {
        assert publicationContextConstructionElapsedMillis != NOT_SET;
        return publicationContextConstructionElapsedMillis;
    }

    /**
     * @return how long in milliseconds it took to commit the publication, i.e. the elapsed time from the publication start until the master
     *         receives publish responses from a majority of master nodes indicating that the state has been received and persisted there.
     */
    public long getPublicationCommitElapsedMillis() {
        assert publicationCommitElapsedMillis != NOT_SET;
        return publicationCommitElapsedMillis;
    }

    /**
     * @return how long in milliseconds it took to complete the publication, i.e. the elapsed time from the publication start until all
     *         nodes except the master have applied the cluster state.
     */
    public long getPublicationCompletionElapsedMillis() {
        assert publicationCompletionElapsedMillis != NOT_SET;
        return publicationCompletionElapsedMillis;
    }

    /**
     * @return how long in milliseconds it took for the master to apply the cluster state, which happens after publication completion.
     */
    public long getMasterApplyElapsedMillis() {
        assert masterApplyElapsedMillis != NOT_SET;
        return masterApplyElapsedMillis;
    }

    /**
     * @return how long in milliseconds it took to construct the publication context, which includes computing a cluster state diff and
     *         serializing the cluster states for future transmission, or zero if not set.
     */
    public long maybeGetPublicationContextConstructionElapsedMillis() {
        return ifSet(publicationContextConstructionElapsedMillis);
    }

    /**
     * @return how long in milliseconds it took to commit the publication, i.e. the elapsed time from the publication start until the master
     *         receives publish responses from a majority of master nodes indicating that the state has been received and persisted there,
     *         or zero if not set.
     */
    public long maybeGetPublicationCommitElapsedMillis() {
        return ifSet(publicationCommitElapsedMillis);
    }

    /**
     * @return how long in milliseconds it took to complete the publication, i.e. the elapsed time from the publication start until all
     *         nodes except the master have applied the cluster state, or zero if not set.
     */
    public long maybeGetPublicationCompletionElapsedMillis() {
        return ifSet(publicationCompletionElapsedMillis);
    }

    /**
     * @return how long in milliseconds it took for the master to apply the cluster state, which happens after publication completion, or
     *         zero if not set.
     */
    public long maybeGetMasterApplyElapsedMillis() {
        return ifSet(masterApplyElapsedMillis);
    }

    private static long ifSet(long millis) {
        assert millis == NOT_SET || millis >= 0;
        return millis == NOT_SET ? 0 : millis;
    }

}
