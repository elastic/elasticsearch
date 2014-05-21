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

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MoreLikeThisFetchService extends AbstractComponent {

    public static final class LikeText {
        public final String field;
        public final String[] text;

        public LikeText(String field, String text) {
            this.field = field;
            this.text = new String[]{text};
        }

        public LikeText(String field, String... text) {
            this.field = field;
            this.text = text;
        }
    }

    private final Client client;

    @Inject
    public MoreLikeThisFetchService(Client client, Settings settings) {
        super(settings);
        this.client = client;
    }

    public List<LikeText> fetch(List<MultiGetRequest.Item> items) throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        for (MultiGetRequest.Item item : items) {
            request.add(item);
        }
        MultiGetResponse responses = client.multiGet(request).actionGet();
        List<LikeText> likeTexts = new ArrayList<>();
        for (MultiGetItemResponse response : responses) {
            if (response.isFailed()) {
                continue;
            }
            GetResponse getResponse = response.getResponse();
            if (!getResponse.isExists()) {
                continue;
            }

            for (GetField getField : getResponse.getFields().values()) {
                String[] text = new String[getField.getValues().size()];
                for (int i = 0; i < text.length; i++) {
                    text[i] = getField.getValues().get(i).toString();
                }
                likeTexts.add(new LikeText(getField.getName(), text));
            }
        }
        return likeTexts;
    }
}
