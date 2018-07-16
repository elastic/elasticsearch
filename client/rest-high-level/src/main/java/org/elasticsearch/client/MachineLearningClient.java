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
package org.elasticsearch.client;

/**
 * A wrapper for the {@link XPackClient} that provides methods for
 * accessing the Elastic Licensed Machine Learning APIs that are shipped with the
 * default distribution of Elasticsearch. All of these APIs will 404 if run
 * against the OSS distribution of Elasticsearch.
 * <p>
 * See the <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-apis.html">
 * X-Pack APIs on elastic.co</a> for more information.
 */
public final class MachineLearningClient {

    private final RestHighLevelClient restHighLevelClient;

    MachineLearningClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }
}
