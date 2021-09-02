/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.search.AbstractSearchTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.JoinHitLookupBuilder;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LookupJoinHitSearchPhaseTests extends AbstractSearchTestCase {

    public void testBasic() throws Exception {
        SearchHit hit1 = new SearchHit(-1, "hit-1",
            Map.of("user_id", newDocField("user_id", "user_1")),
            Map.of());

        SearchHit hit2 = new SearchHit(-1, "hit-2",
            Map.of(
                "user_id", newDocField("user_id", "user_1", "user_2"),
                "product_id", newDocField("product_id", "product_1")),
            Map.of());

        SearchHit hit3 = new SearchHit(-1, "hit-3",
            Map.of("product_id", newDocField("product_id", "product_1")),
            Map.of());
        // Already fetched by the fetch phase
        hit3.addJoinHit("product", new SearchHit.JoinHit("product_1", Map.of(), new BytesArray("name: Elastic")));
        SearchHits searchHits = new SearchHits(new SearchHit[]{hit1, hit2, hit3}, null, 1.0f);
        JoinHitLookupBuilder joinUser = new JoinHitLookupBuilder("user", "user_index", "user_id");
        JoinHitLookupBuilder joinProduct = new JoinHitLookupBuilder("product", "product_index", "product_id");
        JoinHitLookupBuilder joinRating = new JoinHitLookupBuilder("rating", "rating_index", "product_id");

        List<LookupJoinHitSearchPhase.LookupRequest> lookupRequests = LookupJoinHitSearchPhase.getLookupRequests(
            List.of(joinUser, joinProduct, joinRating), searchHits);
        lookupRequests.sort(
            Comparator.comparing(LookupJoinHitSearchPhase.LookupRequest::getIndex)
                .thenComparing(r -> r.getQuery().getMatchField())
                .thenComparing(LookupJoinHitSearchPhase.LookupRequest::getMatchId));
        assertThat(lookupRequests, hasSize(3));
        assertThat(lookupRequests.get(0).getMatchId(), equalTo("product_1"));
        assertThat(lookupRequests.get(0).getQuery(), sameInstance(joinRating));
        assertThat(lookupRequests.get(0).getLeftHits(), equalTo(List.of(hit2, hit3)));
        assertThat(lookupRequests.get(1).getMatchId(), equalTo("user_1"));
        assertThat(lookupRequests.get(1).getQuery(), sameInstance(joinUser));
        assertThat(lookupRequests.get(1).getLeftHits(), equalTo(List.of(hit1, hit2)));
        assertThat(lookupRequests.get(2).getMatchId(), equalTo("user_2"));
        assertThat(lookupRequests.get(2).getQuery(), sameInstance(joinUser));
        assertThat(lookupRequests.get(2).getLeftHits(), equalTo(List.of(hit2)));

        // Verify get requests
        List<MultiGetRequest.Item> getRequests = lookupRequests.stream()
            .map(LookupJoinHitSearchPhase.LookupRequest::toGetRequest)
            .collect(Collectors.toList());
        assertThat(getRequests.get(0).id(), equalTo("product_1"));
        assertThat(getRequests.get(0).index(), sameInstance("rating_index"));
        assertThat(getRequests.get(1).id(), equalTo("user_1"));
        assertThat(getRequests.get(1).index(), sameInstance("user_index"));
        assertThat(getRequests.get(2).id(), equalTo("user_2"));
        assertThat(getRequests.get(2).index(), sameInstance("user_index"));

        // Verify merges
        MultiGetItemResponse[] mResps = new MultiGetItemResponse[3];
        mResps[0] = new MultiGetItemResponse(new GetResponse(newGetResult("rating_index", "product_1", new BytesArray("5 stars"))), null);
        mResps[1] = new MultiGetItemResponse(new GetResponse(nonExistGetResult("user_index", "user_1")), null);
        mResps[2] = new MultiGetItemResponse(new GetResponse(newGetResult("user_index", "user_2", new BytesArray("my_account"))), null);
        LookupJoinHitSearchPhase.mergeJoinHits(lookupRequests, mResps);
        assertThat(hit1.getJoinHits(), aMapWithSize(1));
        assertThat(hit1.getJoinHits().get("user"), hasSize(1));
        assertThat(hit1.getJoinHits().get("user").get(0).getId(), equalTo("user_1"));
        assertThat(hit1.getJoinHits().get("user").get(0).getFailure(), instanceOf(ResourceNotFoundException.class));

        assertThat(hit2.getJoinHits(), aMapWithSize(3));
        assertThat(hit2.getJoinHits().get("user"), hasSize(2));
        assertThat(hit2.getJoinHits().get("user").get(0).getId(), equalTo("user_1"));
        assertThat(hit2.getJoinHits().get("user").get(0).getFailure(), instanceOf(ResourceNotFoundException.class));
        assertThat(hit2.getJoinHits().get("user").get(1).getId(), equalTo("user_2"));
        assertThat(hit2.getJoinHits().get("user").get(1).getFailure(), nullValue());
        assertThat(hit2.getJoinHits().get("user").get(1).getSource(), equalTo(new BytesArray("my_account")));
        assertThat(hit2.getJoinHits().get("product"), hasSize(1));
        assertThat(hit2.getJoinHits().get("product").get(0).getId(), equalTo("product_1"));
        assertThat(hit2.getJoinHits().get("product").get(0).getFailure(), nullValue());
        assertThat(hit2.getJoinHits().get("product").get(0).getSource(), equalTo(new BytesArray("name: Elastic")));
        assertThat(hit2.getJoinHits().get("rating"), hasSize(1));
        assertThat(hit2.getJoinHits().get("rating").get(0).getId(), equalTo("product_1"));
        assertThat(hit2.getJoinHits().get("rating").get(0).getFailure(), nullValue());
        assertThat(hit2.getJoinHits().get("rating").get(0).getSource(), equalTo(new BytesArray("5 stars")));

        assertThat(hit3.getJoinHits(), aMapWithSize(2));
        assertThat(hit3.getJoinHits().get("product"), hasSize(1));
        assertThat(hit3.getJoinHits().get("product").get(0).getId(), equalTo("product_1"));
        assertThat(hit3.getJoinHits().get("product").get(0).getFailure(), nullValue());
        assertThat(hit3.getJoinHits().get("product").get(0).getSource(), equalTo(new BytesArray("name: Elastic")));
        assertThat(hit3.getJoinHits().get("rating"), hasSize(1));
        assertThat(hit3.getJoinHits().get("rating").get(0).getId(), equalTo("product_1"));
        assertThat(hit3.getJoinHits().get("rating").get(0).getFailure(), nullValue());
        assertThat(hit3.getJoinHits().get("rating").get(0).getSource(), equalTo(new BytesArray("5 stars")));
    }

    public void testEmptySearchHit() throws Exception {
        JoinHitLookupBuilder[] specs = new JoinHitLookupBuilder[between(0, 10)];
        for (int i = 0; i < specs.length; i++) {
            specs[i] = new JoinHitLookupBuilder("name-" + i, "index-" + i, "match_field_" + i);
        }
        List<LookupJoinHitSearchPhase.LookupRequest> lookupRequests =
            LookupJoinHitSearchPhase.getLookupRequests(Arrays.asList(specs), new SearchHits(new SearchHit[0], null, 0.0f));
        assertThat(lookupRequests, empty());
    }

    public void testNoJoinRequest() throws Exception {
        SearchHit[] hits = new SearchHit[between(0, 5)];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = new SearchHit(-1, Integer.toString(i), null, Map.of(), Map.of());
        }
        List<LookupJoinHitSearchPhase.LookupRequest> lookupRequests =
            LookupJoinHitSearchPhase.getLookupRequests(List.of(), new SearchHits(hits, null, 0.0f));
        assertThat(lookupRequests, empty());
    }

    static DocumentField newDocField(String name, Object firstValue, Object... moreValues) {
        return new DocumentField(name, CollectionUtils.concatLists(List.of(firstValue), Arrays.asList(moreValues)));
    }

    static GetResult newGetResult(String index, String id, BytesReference source) {
        return new GetResult(index, id, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, 1, true,
            source, Map.of(), Map.of());
    }

    static GetResult nonExistGetResult(String index, String id) {
        return new GetResult(index, id, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, 1, false,
            null, null, null);
    }
}
