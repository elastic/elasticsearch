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

import org.apache.lucene.document.Field;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequest.Item;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.InternalMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.indices.IndicesService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

    private final IndicesService indicesService;

    @Inject
    public MoreLikeThisFetchService(Client client, Settings settings, IndicesService indicesService) {
        super(settings);
        this.client = client;
        this.indicesService = indicesService;
    }

    public List<LikeText> fetch(List<Item> items) throws IOException {
        // create multi-get request
        MultiGetRequest request = new MultiGetRequest();
        for (Item item : items) {
            Item requestItem = new Item(item.index(), item.type(), item.id())
                    .routing(item.routing())
                    .fetchSourceContext(item.fetchSourceContext())
                    .fields(item.fields());
            // no field specified, so add the source to parse
            if (item.fields() == null) {
                requestItem.fields(SourceFieldMapper.NAME);
            }
            request.add(requestItem);
        }

        // perform the query
        MultiGetResponse responses = client.multiGet(request).actionGet();

        // get text from the response
        final List<LikeText> likeTexts = new ArrayList<>();
        int idx = 0;
        for (MultiGetItemResponse response : responses) {
            Item item = items.get(idx++);

            // check the response
            if (response.isFailed()) {
                continue;
            }
            GetResponse getResponse = response.getResponse();
            if (!getResponse.isExists()) {
                throw new DocumentMissingException(null, getResponse.getType(), getResponse.getId());
            }

            // we didn't specify any field, get them from the source
            if (item.fields() == null) {
                parseSource(getResponse, likeTexts);
            } else {  // or from the ones provided
                for (GetField getField : getResponse.getFields().values()) {
                    addToLikeTexts(likeTexts, getField.getName(), getField.getValues());
                }
            }
        }
        return likeTexts;
    }

    private void addToLikeTexts(List<LikeText> likeTexts, String fieldName, List<Object> values) {
        String[] text = new String[values.size()];
        for (int i = 0; i < text.length; i++) {
            text[i] = values.get(i).toString();
        }
        likeTexts.add(new LikeText(fieldName, text));
    }

    private void parseSource(GetResponse getResponse, final List<LikeText> likeTexts) {
        final DocumentMapper docMapper = indicesService.indexServiceSafe(getResponse.getIndex())
                .mapperService().documentMapper(getResponse.getType());
        if (docMapper == null) {
            throw new ElasticsearchException("No DocumentMapper found for type [" + getResponse.getType() + "]");
        }
        if (getResponse.isSourceEmpty()) {
            return;
        }
        SourceToParse sourceToParse = SourceToParse.source(getResponse.getSourceAsBytesRef())
                .type(getResponse.getType())
                .id(getResponse.getId());
        docMapper.parse(sourceToParse, new DocumentMapper.ParseListenerAdapter() {
            @Override
            public boolean beforeFieldAdded(FieldMapper fieldMapper, Field field, Object parseContext) {
                if (!field.fieldType().indexed()) {
                    return false;
                }
                if (fieldMapper instanceof InternalMapper) {
                    return false;
                }
                String value = field.stringValue();
                if (value == null) {
                    return false;
                }
                likeTexts.add(new LikeText(field.name(), value));
                return true;
            }
        });
    }
}
