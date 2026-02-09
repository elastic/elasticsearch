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

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class NewCommitNotificationResponse extends ActionResponse {
    public static final NewCommitNotificationResponse EMPTY = new NewCommitNotificationResponse(Set.of());

    private final Set<PrimaryTermAndGeneration> primaryTermAndGenerationsInUse;

    public NewCommitNotificationResponse(Set<PrimaryTermAndGeneration> primaryTermAndGenerationsInUse) {
        this.primaryTermAndGenerationsInUse = primaryTermAndGenerationsInUse;
    }

    public NewCommitNotificationResponse(StreamInput in) throws IOException {
        this.primaryTermAndGenerationsInUse = in.readCollectionAsImmutableSet(PrimaryTermAndGeneration::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(primaryTermAndGenerationsInUse);
    }

    public Set<PrimaryTermAndGeneration> getPrimaryTermAndGenerationsInUse() {
        return primaryTermAndGenerationsInUse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NewCommitNotificationResponse that = (NewCommitNotificationResponse) o;
        return Objects.equals(primaryTermAndGenerationsInUse, that.primaryTermAndGenerationsInUse);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTermAndGenerationsInUse);
    }

    public static NewCommitNotificationResponse combine(List<NewCommitNotificationResponse> responses) {
        var combinedPrimaryTermAndGenerations = new HashSet<PrimaryTermAndGeneration>();
        for (NewCommitNotificationResponse response : responses) {
            combinedPrimaryTermAndGenerations.addAll(response.primaryTermAndGenerationsInUse);
        }
        return new NewCommitNotificationResponse(Collections.unmodifiableSet(combinedPrimaryTermAndGenerations));
    }

    @Override
    public String toString() {
        return "NewCommitNotificationResponse{" + "primaryTermAndGenerationsInUse=" + primaryTermAndGenerationsInUse + "}";
    }
}
