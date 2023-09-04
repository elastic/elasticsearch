/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.action;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class NewCommitNotificationResponse extends ActionResponse {
    public static final NewCommitNotificationResponse EMPTY = new NewCommitNotificationResponse(Set.of());

    private static final TransportVersion VERSION_SUPPORTING_NEW_COMMIT_NOTIFICATION_RESPONSE = TransportVersion.V_8_500_061;

    private final Set<PrimaryTermAndGeneration> usedPrimaryTermAndGenerations;

    public NewCommitNotificationResponse(Set<PrimaryTermAndGeneration> usedPrimaryTermAndGenerations) {
        this.usedPrimaryTermAndGenerations = usedPrimaryTermAndGenerations;
    }

    public NewCommitNotificationResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(VERSION_SUPPORTING_NEW_COMMIT_NOTIFICATION_RESPONSE)) {
            this.usedPrimaryTermAndGenerations = in.readCollectionAsImmutableSet(PrimaryTermAndGeneration::new);
        } else {
            this.usedPrimaryTermAndGenerations = Collections.emptySet();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Before this version we used ActionResponse.Empty as a response
        if (out.getTransportVersion().onOrAfter(VERSION_SUPPORTING_NEW_COMMIT_NOTIFICATION_RESPONSE)) {
            out.writeCollection(usedPrimaryTermAndGenerations);
        }
    }

    public Set<PrimaryTermAndGeneration> getUsedPrimaryTermAndGenerations() {
        return usedPrimaryTermAndGenerations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewCommitNotificationResponse that = (NewCommitNotificationResponse) o;
        return Objects.equals(usedPrimaryTermAndGenerations, that.usedPrimaryTermAndGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(usedPrimaryTermAndGenerations);
    }

    public static NewCommitNotificationResponse combine(List<NewCommitNotificationResponse> responses) {
        var combinedPrimaryTermAndGenerations = new HashSet<PrimaryTermAndGeneration>();
        for (NewCommitNotificationResponse response : responses) {
            combinedPrimaryTermAndGenerations.addAll(response.usedPrimaryTermAndGenerations);
        }
        return new NewCommitNotificationResponse(Collections.unmodifiableSet(combinedPrimaryTermAndGenerations));
    }
}
