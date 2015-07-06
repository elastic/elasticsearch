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

package org.elasticsearch.index.search.termslookup;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.cache.query.terms.TermsLookup;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Service which retrieves terms from a {@link TermsLookup} specification
 */
public class TermsLookupFetchService extends AbstractComponent {

    private final Client client;

    @Inject
    public TermsLookupFetchService(Client client, Settings settings) {
        super(settings);
        this.client = client;
    }

    public List<Object> fetch(TermsLookup termsLookup) {
        List<Object> terms = new ArrayList<>();
        GetRequest getRequest = new GetRequest(termsLookup.index(), termsLookup.type(), termsLookup.id())
                .preference("_local").routing(termsLookup.routing());
        getRequest.copyContextAndHeadersFrom(SearchContext.current());
        final GetResponse getResponse = client.get(getRequest).actionGet();
        if (getResponse.isExists()) {
            List<Object> extractedValues = XContentMapValues.extractRawValues(termsLookup.path(), getResponse.getSourceAsMap());
            terms.addAll(extractedValues);
        }
        return terms;
    }
}
