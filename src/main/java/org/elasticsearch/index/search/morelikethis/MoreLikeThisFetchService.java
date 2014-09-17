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

package org.elasticsearch.index.search.morelikethis;

import org.apache.lucene.index.Fields;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsItemResponse;
import org.elasticsearch.action.termvector.MultiTermVectorsRequest;
import org.elasticsearch.action.termvector.MultiTermVectorsResponse;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MoreLikeThisFetchService extends AbstractComponent {

    private final Client client;

    @Inject
    public MoreLikeThisFetchService(Client client, Settings settings) {
        super(settings);
        this.client = client;
    }

    public Fields[] fetch(List<MultiGetRequest.Item> items) throws IOException {
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        for (MultiGetRequest.Item item : items) {
            request.add(item);
        }
        List<Fields> likeFields = new ArrayList<>();
        MultiTermVectorsResponse responses = client.multiTermVectors(request).actionGet();
        for (MultiTermVectorsItemResponse response : responses) {
            if (response.isFailed()) {
                continue;
            }
            TermVectorResponse getResponse = response.getResponse();
            if (!getResponse.isExists()) {
                continue;
            }
            likeFields.add(getResponse.getFields());
        }
        return likeFields.toArray(Fields.EMPTY_ARRAY);
    }
}
